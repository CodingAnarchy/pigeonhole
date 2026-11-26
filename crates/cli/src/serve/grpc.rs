//! gRPC: `Data`'s RPCs, decoded by tonic and answered by the Router. The token comes from the
//! `authorization` metadata, checked as for HTTP.
//!
//! **The in-flight budget is reserved before decoding** (perf287's F2 on #1501): `prepay`
//! reads a request's 5-byte message header, reserves the message's length, and only then
//! lets tonic read and decode the body. So a request waiting for the budget holds the data
//! frames read so far, not its message: at most the HTTP/2 stream window (64 KiB by default)
//! per stream, with `listener::MAX_STREAMS` streams per connection.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, BodyDataStream, Bytes};
use axum::extract::{Request as HttpRequest, State};
use axum::middleware::Next;
use axum::response::Response as HttpResponse;
use futures_core::Stream;
use tokio::sync::OwnedSemaphorePermit;
use tonic::codegen::http;
use tonic::{Request, Response, Status};

use super::errors::ApiError;
use super::feed::Out;
use super::proto::{self, data_server::Data};
use super::replica::ReadDeadline;
use super::service::{ServiceOp, ServiceReply};
use super::{Shared, auth};

/// The budget `prepay` reserved for a request, for its handler to take. Extensions must be
/// `Clone`; the permit is taken once.
#[derive(Clone, Default)]
struct Prepaid(Arc<Mutex<Option<OwnedSemaphorePermit>>>);

/// The caller `prepay` identified from the request's token, for its handler to take, so a
/// gRPC call hashes and looks up its token once, not twice (#1828: 168 ns a call). Removed
/// by [`caller_of`]; extensions must be `Clone`.
#[derive(Clone)]
struct Authed(auth::Caller);

/// The caller of a gRPC request: the one [`prepay`] identified, taken from the request's
/// extensions, or (a route not behind `prepay`) from its token now.
fn caller_of<T>(shared: &Shared, req: &mut Request<T>) -> Result<auth::Caller, ApiError> {
    let mut caller = match req.extensions_mut().remove::<Authed>() {
        Some(Authed(caller)) => caller,
        None => shared.caller(auth_header(req))?,
    };
    // D335: a write that asked not to wait for its partition's replica acks. Only a cluster
    // node reads the header: a single node's calls keep their cost.
    if shared.router.is_cluster() {
        caller.replication_async = super::acks::replication_async(
            req.metadata()
                .get(super::acks::REPLICATION_HEADER)
                .map(|v| v.as_bytes()),
        )?;
    }
    Ok(caller)
}

/// When `prepay` (or, over HTTP/1.1, `preread`) saw the request arrive: the start of its one
/// deadline, which the budget wait and a replica's wait for the client's primary seqno share
/// (cli692's R1 on #1594), and an HTTP/1.1 body's pre-read too (perf287 on #1613).
#[derive(Clone, Copy)]
pub(crate) struct Arrived(pub(crate) Instant);

impl Prepaid {
    fn take(&self) -> Option<OwnedSemaphorePermit> {
        self.0.lock().ok().and_then(|mut p| p.take())
    }
}

/// Middleware on the gRPC routes: reserves the in-flight budget from the message header
/// before tonic decodes the message. The wait is bounded by the request timeout, and by the
/// client's `grpc-timeout` when that is shorter (`DeadlineExceeded` then). A message longer
/// than `--max-request-bytes` reserves only that much; tonic refuses it as it decodes.
///
/// The header's length is the message as sent, compressed or not. tonic isn't configured to
/// accept compressed requests (`accept_compressed`), so it refuses them and the reservation
/// is exact; enabling compression would need the decoded bound,
/// `max_decoding_message_size`, reserved instead.
///
/// A client stream (`Vectors.BulkLoad`) reserves each later message the same way, from its
/// header, as tonic reads on ([`Metered`]); its handler takes the reservations in order.
///
/// **The token is checked first** (audit120's should 1): a request without a valid token is
/// `Unauthenticated` before any of its body is read or any budget reserved, so it can't hold
/// either. (`Admin.Health` takes no token, on a route of its own: [`health_prepay`].) The
/// handler checks the scope later, as for HTTP.
/// **The message's read is bounded** by the request's one deadline (the request timeout, or
/// a shorter `grpc-timeout`): `prepay` reads the header, reserves, then reads the rest of
/// the first message before tonic decodes it, so a client that sends a call's headers but
/// not its message gets `DeadlineExceeded` instead of holding its `--max-concurrent` place
/// and its bytes (audit120's S-S1). A client stream's later messages each come within
/// `--stream-idle-ms` of the last chunk ([`Metered`]).
pub(crate) async fn prepay(
    State(shared): State<Arc<Shared>>,
    req: HttpRequest,
    next: Next,
) -> HttpResponse {
    let arrived = Instant::now();
    let (parts, body) = req.into_parts();
    let token = parts
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let caller = match shared.caller(token) {
        Ok(caller) => caller,
        Err(e) => return status(e),
    };
    let limits = shared.limits;
    let deadline = grpc_timeout(&parts.headers);
    let wait = deadline.map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
    // The request's one deadline: the header, the reservation and the first message all
    // come within it.
    let until = arrived + wait;
    let mut data = body.into_data_stream();
    let mut head = Vec::new();
    if let Err(e) = read_to(&mut data, &mut head, 5, until).await {
        return status(e);
    }
    let length = head
        .get(1..5)
        .map_or(0, |b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize);
    let bytes = length.min(limits.max_request_bytes);
    let left = until.saturating_duration_since(Instant::now());
    let permit = match shared.router.service.reserve_within(bytes, left).await {
        Ok(p) => p,
        // The client's own deadline ran out first.
        Err(_) if deadline.is_some_and(|d| d <= limits.request_timeout) => {
            return status(ApiError::deadline_exceeded());
        }
        Err(e) => return status(e),
    };
    // The first message's bytes, once their share is reserved; one longer than
    // `--max-request-bytes` is left for tonic to refuse from its header. The buffer is sized
    // from the header once, not grown chunk by chunk (#1828: 1.5 µs at 64 KiB, 0.96 ms at
    // 16 MiB).
    if length <= limits.max_request_bytes {
        head.reserve_exact((5 + length).saturating_sub(head.len()));
        if let Err(e) = read_to(&mut data, &mut head, 5 + length, until).await {
            return status(e);
        }
    }
    let rest = Rest {
        head: Some(Bytes::from(head)),
        rest: data,
    };
    let meter = Meter::default();
    let body = if parts.uri.path() == BULK_LOAD || parts.uri.path() == FORWARD_BULK_LOAD {
        Body::from_stream(Metered {
            inner: rest,
            meter: meter.clone(),
            shared: Arc::clone(&shared),
            wait,
            left: 5 + length,
            head: Vec::new(),
            held: Bytes::new(),
            reserving: None,
            idle: Box::pin(tokio::time::sleep(limits.stream_idle)),
            idle_for: limits.stream_idle,
        })
    } else {
        Body::from_stream(rest)
    };
    let mut req = HttpRequest::from_parts(parts, body);
    req.extensions_mut()
        .insert(Prepaid(Arc::new(Mutex::new(Some(permit)))));
    req.extensions_mut().insert(Arrived(arrived));
    req.extensions_mut().insert(Authed(caller));
    req.extensions_mut().insert(meter);
    // A unary read's reply charge (#1567) moves into its body here, on the way out.
    super::charge::hold(next.run(req).await, limits.stream_idle)
}

/// `Admin.Health`, which takes no token: routed apart, through [`health_prepay`].
pub(crate) const HEALTH: &str = "/pigeonhole.v1.Admin/Health";

/// The longest message `Admin.Health` takes. Its request is `Empty`, so this is room only
/// for fields a later version might add.
const HEALTH_MESSAGE: usize = 64;

/// Middleware on `Admin.Health`'s own route, outside `--max-concurrent` as `/v1/health` is,
/// so a probe answers while requests are shed (audit120's P2-F3). It takes no token, so it
/// reserves nothing: a declared message over [`HEALTH_MESSAGE`] bytes is refused from its
/// header, and the message is read within the request's deadline. So header-only calls to
/// it, without a token, can't hold the in-flight budget or a request place.
pub(crate) async fn health_prepay(
    State(shared): State<Arc<Shared>>,
    req: HttpRequest,
    next: Next,
) -> HttpResponse {
    let arrived = Instant::now();
    let (parts, body) = req.into_parts();
    let limits = shared.limits;
    let wait = grpc_timeout(&parts.headers)
        .map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
    let until = arrived + wait;
    let mut data = body.into_data_stream();
    let mut head = Vec::new();
    if let Err(e) = read_to(&mut data, &mut head, 5, until).await {
        return status(e);
    }
    let length = head
        .get(1..5)
        .map_or(0, |b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize);
    if length > HEALTH_MESSAGE {
        return status(ApiError::invalid(format!(
            "Admin.Health takes an empty message, not {length} bytes"
        )));
    }
    if let Err(e) = read_to(&mut data, &mut head, 5 + length, until).await {
        return status(e);
    }
    let body = Body::from_stream(Rest {
        head: Some(Bytes::from(head)),
        rest: data,
    });
    let mut req = HttpRequest::from_parts(parts, body);
    req.extensions_mut().insert(Arrived(arrived));
    next.run(req).await
}

/// The client stream whose messages are each reserved as they arrive.
const BULK_LOAD: &str = "/pigeonhole.v1.Vectors/BulkLoad";

/// A partition's part of a cluster's `BulkLoad` from another node (#1722 PR 3): a client
/// stream too, each message reserved as it arrives. Mounted on a route of its own behind
/// [`prepay`] (`cluster::routes`).
pub(crate) const FORWARD_BULK_LOAD: &str = "/pigeonhole.v1.Cluster/ForwardBulkLoad";

/// The reservations [`Metered`] made for a stream's later messages, in order, for its
/// handler to take one per message; or the reservation that failed.
#[derive(Clone, Default)]
pub(crate) struct Meter(
    Arc<
        Mutex<(
            std::collections::VecDeque<OwnedSemaphorePermit>,
            Option<ApiError>,
        )>,
    >,
);

impl Meter {
    fn lock(
        &self,
    ) -> std::sync::MutexGuard<
        '_,
        (
            std::collections::VecDeque<OwnedSemaphorePermit>,
            Option<ApiError>,
        ),
    > {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

type Reserving =
    Pin<Box<dyn std::future::Future<Output = Result<OwnedSemaphorePermit, ApiError>> + Send>>;

/// A client stream's body after `prepay`: each message after the first has its length
/// reserved from its 5-byte header before its bytes pass on to tonic (perf287's (A) on 7b2).
/// The bytes read meanwhile wait here: at most a chunk, as the HTTP/2 window allows.
struct Metered {
    inner: Rest,
    meter: Meter,
    shared: Arc<Shared>,
    wait: Duration,
    /// The current message's bytes, header included, still to pass.
    left: usize,
    /// The next message's header, gathered across chunks.
    head: Vec<u8>,
    /// Bytes read, not yet passed.
    held: Bytes,
    reserving: Option<Reserving>,
    /// When the client has sent nothing for `idle_for` (`--stream-idle-ms`), the stream
    /// ends with `DeadlineExceeded` (audit120's S-S1).
    idle: Pin<Box<tokio::time::Sleep>>,
    idle_for: Duration,
}

impl Stream for Metered {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(r) = &mut this.reserving {
                match r.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => {
                        this.reserving = None;
                        match r {
                            Ok(p) => this.meter.lock().0.push_back(p),
                            Err(e) => {
                                this.meter.lock().1 = Some(e);
                                return Poll::Ready(Some(Err(axum::Error::new(
                                    "the in-flight budget stayed full",
                                ))));
                            }
                        }
                    }
                }
            }
            if this.held.is_empty() {
                match Pin::new(&mut this.inner).poll_next(cx) {
                    Poll::Ready(Some(Ok(b))) => {
                        this.held = b;
                        let next = tokio::time::Instant::now() + this.idle_for;
                        this.idle.as_mut().reset(next);
                    }
                    Poll::Pending => {
                        if this.idle.as_mut().poll(cx).is_ready() {
                            this.meter.lock().1 = Some(ApiError::deadline(format!(
                                "the client sent nothing for --stream-idle-ms ({} ms)",
                                this.idle_for.as_millis()
                            )));
                            return Poll::Ready(Some(Err(axum::Error::new(
                                "the client stream went idle",
                            ))));
                        }
                        return Poll::Pending;
                    }
                    other => return other,
                }
                continue;
            }
            if this.left > 0 {
                let out = this.held.split_to(this.left.min(this.held.len()));
                this.left -= out.len();
                return Poll::Ready(Some(Ok(out)));
            }
            // A message boundary: the next header, then its reservation, then its bytes.
            let take = (5 - this.head.len()).min(this.held.len());
            this.head.extend_from_slice(&this.held.split_to(take));
            if this.head.len() == 5 {
                let head = std::mem::take(&mut this.head);
                let length = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
                this.left = length;
                let bytes = length.min(this.shared.limits.max_request_bytes);
                let (shared, wait) = (Arc::clone(&this.shared), this.wait);
                this.reserving = Some(Box::pin(async move {
                    shared.router.service.reserve_within(bytes, wait).await
                }));
                return Poll::Ready(Some(Ok(Bytes::from(head))));
            }
        }
    }
}

/// Reads `data` into `buf` until it holds `n` bytes or the body ends (tonic answers a short
/// one), by `until`: past it, `DeadlineExceeded`, as every deadline in `phdb serve` is.
async fn read_to(
    data: &mut BodyDataStream,
    buf: &mut Vec<u8>,
    n: usize,
    until: Instant,
) -> Result<(), ApiError> {
    let read = async {
        while buf.len() < n {
            match std::future::poll_fn(|cx| Pin::new(&mut *data).poll_next(cx)).await {
                Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                Some(Err(_)) => return Err(ApiError::invalid("the request body broke off")),
                None => break,
            }
        }
        Ok(())
    };
    tokio::time::timeout_at(until.into(), read)
        .await
        .unwrap_or_else(|_| Err(ApiError::deadline_exceeded()))
}

fn status(e: ApiError) -> HttpResponse {
    let (parts, body) = e.to_status().into_http::<tonic::body::Body>().into_parts();
    HttpResponse::from_parts(parts, Body::new(body))
}

/// A body's bytes after `prepay` read its head: the head, then the rest.
struct Rest {
    head: Option<Bytes>,
    rest: BodyDataStream,
}

impl Stream for Rest {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(head) = self.head.take()
            && !head.is_empty()
        {
            return Poll::Ready(Some(Ok(head)));
        }
        Pin::new(&mut self.rest).poll_next(cx)
    }
}

/// The `grpc-timeout` header: digits and a unit (`H`, `M`, `S`, `m`, `u`, `n`).
pub(crate) fn grpc_timeout(headers: &axum::http::HeaderMap) -> Option<Duration> {
    parse_grpc_timeout(headers.get("grpc-timeout")?.to_str().ok()?)
}

/// A `grpc-timeout` value: digits and a unit.
fn parse_grpc_timeout(v: &str) -> Option<Duration> {
    let (n, unit) = v.split_at(v.len().checked_sub(1)?);
    let n: u64 = n.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.saturating_mul(3600)),
        "M" => Duration::from_secs(n.saturating_mul(60)),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

/// A gRPC request's `Authorization` header, if it has one that is text.
pub(crate) fn auth_header<T>(req: &Request<T>) -> Option<&str> {
    req.metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
}

/// A reply stream as gRPC messages: each `ApiError` as its `Status`. A scan's pages (a
/// cluster's forwarded scan's too, D317 ticket 4a), a backup download's chunks, a follow's
/// batches and a memory watch's events.
pub(crate) struct GrpcStream<S>(pub(crate) S);

impl<S, T> Stream for GrpcStream<S>
where
    S: Stream<Item = Result<T, ApiError>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map(|item| item.map(|r| r.map_err(|e| e.to_status())))
    }
}

/// `Data` over gRPC.
#[derive(Debug, Clone)]
pub(crate) struct DataGrpc(pub(crate) Arc<Shared>);

impl DataGrpc {
    async fn call<T>(
        &self,
        req: Request<T>,
        op: impl FnOnce(T) -> ServiceOp,
    ) -> Result<ServiceReply, Status> {
        call(&self.0, req, op).await
    }
}

tokio::task_local! {
    /// What served a read a replica could have (D328), set by [`call`] on a cluster node
    /// for [`served_headers`] to put on the response.
    static SERVED: std::cell::Cell<Option<super::replica::Served>>;
}

/// A cluster node's `Data` route's middleware (D328): the `served-by` and `served-seqno`
/// headers of a read a replica could have served. Only a cluster node mounts it, so a
/// single node's calls are as they were (the serve callgrind shapes, D193).
pub(crate) async fn served_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    SERVED
        .scope(std::cell::Cell::new(None), async move {
            let mut resp = next.run(req).await;
            if let Some(served) = SERVED.with(std::cell::Cell::get) {
                for (name, value) in served.headers() {
                    if let Ok(value) = value.parse() {
                        resp.headers_mut().insert(name, value);
                    }
                }
            }
            resp
        })
        .await
}

/// One gRPC call through the router, for every service.
async fn call<T>(
    shared: &Shared,
    mut req: Request<T>,
    op: impl FnOnce(T) -> ServiceOp,
) -> Result<ServiceReply, Status> {
    // A cluster node's calls may carry the replica header (D328): boxed, so a single node's
    // calls keep the future and the cost they had.
    if shared.router.is_cluster() {
        let (reply, served) = Box::pin(call_served(shared, req, op)).await?;
        if let Some(served) = served {
            // For [`served_headers`], which wraps a cluster node's `Data` route.
            let _ = SERVED.try_with(|slot| slot.set(Some(served)));
        }
        return Ok(reply);
    }
    let caller = caller_of(shared, &mut req)?;
    let prepaid = req.extensions().get::<Prepaid>().and_then(Prepaid::take);
    let started = Instant::now();
    let min_primary_seqno = super::replica::min_primary_seqno(
        req.metadata()
            .get(super::replica::MIN_PRIMARY_SEQNO)
            .map(|v| v.as_bytes()),
    );
    // The request's one deadline, from its arrival (#1577).
    let arrived = req.extensions().get::<Arrived>().map_or(started, |a| a.0);
    let deadline = ReadDeadline::new(
        arrived,
        shared.limits.request_timeout,
        grpc_timeout(req.metadata().as_ref()),
    );
    let op = op(req.into_inner());
    let rpc = op.index();
    let reply = shared
        .router
        .dispatch_after_primary(op, &caller, prepaid, min_primary_seqno, deadline)
        .await;
    shared.count(rpc, started, &reply);
    Ok(reply?)
}

/// [`call`] on a cluster node, with what served a read a replica could have (D328), for its
/// headers. A single node's calls never come here, so their cost stays as it was (the
/// serve callgrind shapes, D193).
async fn call_served<T>(
    shared: &Shared,
    mut req: Request<T>,
    op: impl FnOnce(T) -> ServiceOp,
) -> Result<(ServiceReply, Option<super::replica::Served>), Status> {
    let caller = caller_of(shared, &mut req)?;
    let prepaid = req.extensions().get::<Prepaid>().and_then(Prepaid::take);
    let started = Instant::now();
    let min_primary_seqno = super::replica::min_primary_seqno(
        req.metadata()
            .get(super::replica::MIN_PRIMARY_SEQNO)
            .map(|v| v.as_bytes()),
    );
    // Only a cluster node reads the replica header (D328): a single node's calls keep
    // their cost (the serve callgrind shapes, D193).
    let max_lag = if shared.router.is_cluster() {
        super::replica::read_replica_max_lag(
            req.metadata()
                .get(super::replica::READ_REPLICA_MAX_LAG_MS)
                .map(|v| v.as_bytes()),
        )
    } else {
        Ok(None)
    };
    // The request's one deadline, from its arrival (#1577).
    let arrived = req.extensions().get::<Arrived>().map_or(started, |a| a.0);
    let deadline = ReadDeadline::new(
        arrived,
        shared.limits.request_timeout,
        grpc_timeout(req.metadata().as_ref()),
    );
    let op = op(req.into_inner());
    let rpc = op.index();
    // A call without the header goes as it always did; one with it, boxed, so its future
    // doesn't widen every call's.
    let (reply, served) = match max_lag {
        Ok(None) => (
            shared
                .router
                .dispatch_after_primary(op, &caller, prepaid, min_primary_seqno, deadline)
                .await,
            None,
        ),
        max_lag => match Box::pin(shared.router.dispatch_read(
            op,
            &caller,
            prepaid,
            min_primary_seqno,
            max_lag,
            deadline,
        ))
        .await
        {
            Ok((reply, served)) => (Ok(reply), served),
            Err(e) => (Err(e), None),
        },
    };
    shared.count(rpc, started, &reply);
    Ok((reply?, served))
}

/// A unary RPC: the call, and its reply's variant.
macro_rules! unary {
    ($shared:expr, $req:expr, $op:ident, $reply:ident) => {
        respond(call($shared, $req, ServiceOp::$op).await?, |r| match r {
            ServiceReply::$reply(r) => Some(r),
            _ => None,
        })
    };
}

/// A unary reply as its response, a read's charge (#1567) in its extensions, which the
/// gRPC routes' middleware moves into the body on the way out ([`super::charge::hold`]).
/// Only valid for a route behind [`prepay`]: anywhere else nothing moves the charge into
/// the body, so it is released when the extensions drop, before the reply is sent.
fn respond<T>(
    reply: ServiceReply,
    pick: impl FnOnce(ServiceReply) -> Option<T>,
) -> Result<Response<T>, Status> {
    let (reply, charge) = reply.uncharge();
    let mut resp = Response::new(pick(reply).ok_or_else(unexpected)?);
    if let Some(c) = charge {
        resp.extensions_mut().insert(super::charge::Charged::new(c));
    }
    Ok(resp)
}

/// `Transactions` over gRPC (D313 §3).
#[derive(Debug, Clone)]
pub(crate) struct TxnGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::transactions_server::Transactions for TxnGrpc {
    async fn begin(
        &self,
        req: Request<proto::BeginRequest>,
    ) -> Result<Response<proto::BeginResponse>, Status> {
        unary!(&self.0, req, Begin, Begin)
    }

    async fn get(
        &self,
        req: Request<proto::TxnGetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        unary!(&self.0, req, TxnGet, Get)
    }

    async fn get_row(
        &self,
        req: Request<proto::TxnGetRowRequest>,
    ) -> Result<Response<proto::ReadRowResponse>, Status> {
        unary!(&self.0, req, TxnGetRow, ReadRow)
    }

    async fn write(
        &self,
        req: Request<proto::TxnWriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, TxnWrite, Empty)
    }

    async fn commit(
        &self,
        req: Request<proto::TxnCommitRequest>,
    ) -> Result<Response<proto::TxnCommitResponse>, Status> {
        unary!(&self.0, req, Commit, Commit)
    }

    async fn abort(
        &self,
        req: Request<proto::TxnAbortRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Abort, Empty)
    }

    async fn outcome(
        &self,
        req: Request<proto::TxnOutcomeRequest>,
    ) -> Result<Response<proto::TxnOutcomeResponse>, Status> {
        unary!(&self.0, req, Outcome, Outcome)
    }
}

/// `Feed` over gRPC (D313 R2): the fan-out's events, an error as the stream's final status.
///
/// Routed by hand ([`EventsGrpc`]) rather than through the generated `FeedServer`, for its
/// codec: a ring entry sent to every client with no prefix is written as the bytes it was
/// encoded to once, not built and encoded again per subscriber (perf287's profile on
/// #1536). The wire format is the generated one; `Feed` has the one RPC.
#[derive(Debug, Clone)]
pub(crate) struct FeedGrpc {
    pub(crate) shared: Arc<Shared>,
    /// `--max-request-bytes`, for decoding the request.
    pub(crate) max_request_bytes: usize,
}

pub(crate) struct GrpcEvents(pub(crate) super::feed::FeedEvents);

impl Stream for GrpcEvents {
    type Item = Result<Out, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_out(cx)
            .map(|item| item.map(|r| r.map_err(|e| e.to_status())))
    }
}

/// A feed stream's codec, for `Feed.Subscribe` and a cluster's `ForwardSubscribe`: a
/// request `R` is decoded as the generated codec does, and events are written by
/// [`Out::encode`].
struct OutCodec<R>(std::marker::PhantomData<fn() -> R>);

impl<R: prost::Message + Default + Send + 'static> tonic::codec::Codec for OutCodec<R> {
    type Encode = Out;
    type Decode = R;
    type Encoder = OutEncoder;
    type Decoder = tonic_prost::ProstDecoder<R>;

    fn encoder(&mut self) -> Self::Encoder {
        OutEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstDecoder::default()
    }
}

struct OutEncoder;

impl tonic::codec::Encoder for OutEncoder {
    type Item = Out;
    type Error = Status;

    fn encode(&mut self, item: Out, dst: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Status> {
        // The buffer grows as needed: as for the generated codec, encoding can't run out.
        item.encode(dst)
            .map_err(|e| Status::internal(format!("encoding a feed event: {e}")))
    }
}

/// What answers a feed RPC routed by hand ([`EventsGrpc`]): `Feed.Subscribe`, and a cluster
/// node's `ForwardSubscribe`.
pub(crate) trait Events: Clone + Send + 'static {
    type Request: prost::Message + Default + Send + 'static;

    /// The longest request it decodes.
    fn max_request_bytes(&self) -> usize;

    /// Its answer to a request for `path` it doesn't serve; `None` for one it does. A
    /// service mounted on its one method's path serves whatever reaches it.
    fn refuses(_path: &str) -> Option<Status> {
        None
    }

    /// The request's events.
    fn events(
        self,
        req: Request<Self::Request>,
    ) -> tonic::codegen::BoxFuture<Response<GrpcEvents>, Status>;
}

/// One server-streaming feed RPC over gRPC, routed by hand for its codec ([`OutCodec`]):
/// the shell around an [`Events`].
#[derive(Debug, Clone)]
pub(crate) struct EventsGrpc<E>(pub(crate) E);

struct EventsSvc<E>(E);

impl<E: Events> tonic::server::ServerStreamingService<E::Request> for EventsSvc<E> {
    type Response = Out;
    type ResponseStream = GrpcEvents;
    type Future = tonic::codegen::BoxFuture<Response<GrpcEvents>, Status>;

    fn call(&mut self, req: Request<E::Request>) -> Self::Future {
        self.0.clone().events(req)
    }
}

impl<E, B> tower::Service<http::Request<B>> for EventsGrpc<E>
where
    E: Events,
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        if let Some(refused) = E::refuses(req.uri().path()) {
            return Box::pin(async move { Ok(refused.into_http()) });
        }
        let events = self.0.clone();
        Box::pin(async move {
            let codec = OutCodec::<E::Request>(std::marker::PhantomData);
            let mut grpc = tonic::server::Grpc::new(codec)
                .apply_max_message_size_config(Some(events.max_request_bytes()), Some(usize::MAX));
            Ok(grpc.server_streaming(EventsSvc(events), req).await)
        })
    }
}

impl Events for FeedGrpc {
    type Request = proto::SubscribeRequest;

    fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    fn refuses(path: &str) -> Option<Status> {
        (path != "/pigeonhole.v1.Feed/Subscribe")
            .then(|| Status::unimplemented("no such method in pigeonhole.v1.Feed"))
    }

    fn events(
        self,
        req: Request<proto::SubscribeRequest>,
    ) -> tonic::codegen::BoxFuture<Response<GrpcEvents>, Status> {
        Box::pin(async move {
            match call(&self.shared, req, ServiceOp::Subscribe).await? {
                ServiceReply::Feed(events) => Ok(Response::new(GrpcEvents(events))),
                _ => Err(unexpected()),
            }
        })
    }
}

impl tonic::server::NamedService for EventsGrpc<FeedGrpc> {
    const NAME: &'static str = proto::feed_server::SERVICE_NAME;
}

/// `Admin` over gRPC (D313 ticket 6).
#[derive(Debug, Clone)]
pub(crate) struct AdminGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::admin_server::Admin for AdminGrpc {
    /// Liveness, with no token: whether the database is open and not failed.
    async fn health(
        &self,
        _req: Request<proto::Empty>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        // Every database this server serves (a cluster node's partitions, D317 ticket 8a), as
        // `/v1/health` checks; a cluster node that only routes is up.
        let ok = self.0.router.parts().iter().all(|p| !p.db.is_failed());
        Ok(Response::new(proto::HealthResponse {
            status: if ok { "ok" } else { "failed" }.into(),
        }))
    }

    async fn info(
        &self,
        req: Request<proto::Empty>,
    ) -> Result<Response<proto::InfoResponse>, Status> {
        unary!(&self.0, req, Info, Info)
    }

    async fn flush(
        &self,
        req: Request<proto::MaintenanceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Flush, Empty)
    }

    async fn compact(
        &self,
        req: Request<proto::MaintenanceRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Compact, Empty)
    }

    async fn backup(
        &self,
        req: Request<proto::BackupRequest>,
    ) -> Result<Response<proto::BackupResponse>, Status> {
        unary!(&self.0, req, Backup, Backup)
    }

    type BackupDownloadStream =
        Pin<Box<dyn Stream<Item = Result<proto::BackupChunk, Status>> + Send>>;

    async fn backup_download(
        &self,
        req: Request<proto::BackupDownloadRequest>,
    ) -> Result<Response<Self::BackupDownloadStream>, Status> {
        match call(&self.0, req, ServiceOp::BackupDownload).await? {
            ServiceReply::Download(d) => Ok(Response::new(Box::pin(GrpcStream(d)))),
            _ => Err(unexpected()),
        }
    }

    async fn metrics(
        &self,
        req: Request<proto::Empty>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        unary!(&self.0, req, Metrics, Metrics)
    }

    async fn promote(
        &self,
        req: Request<proto::Empty>,
    ) -> Result<Response<proto::PromoteResponse>, Status> {
        unary!(&self.0, req, Promote, Promote)
    }

    async fn partition_upload(
        &self,
        mut req: Request<tonic::Streaming<proto::PartitionUploadChunk>>,
    ) -> Result<Response<proto::PartitionUploadReply>, Status> {
        let caller = caller_of(&self.0, &mut req)?;
        let reply = self
            .0
            .router
            .partition_upload(&caller, req.into_inner())
            .await?;
        Ok(Response::new(reply))
    }

    async fn partition_upload_status(
        &self,
        mut req: Request<proto::PartitionUploadStatusRequest>,
    ) -> Result<Response<proto::PartitionUploadStatusReply>, Status> {
        let caller = caller_of(&self.0, &mut req)?;
        let reply = self.0.router.partition_upload_status(
            &caller,
            &req.get_ref().upload_id,
            &req.get_ref().partition,
        )?;
        Ok(Response::new(reply))
    }
}

/// `Replication` over gRPC (#1436): a replica server follows its primary here.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::replication_server::Replication for ReplicationGrpc {
    type FollowStream = Pin<Box<dyn Stream<Item = Result<proto::ReplicationBatch, Status>> + Send>>;

    async fn follow(
        &self,
        req: Request<proto::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        // Which node follows, for the owner's map-replica check (D329 2b).
        let node = req
            .metadata()
            .get(super::cluster::NODE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let followed = super::router::FOLLOWER.scope(node, call(&self.0, req, ServiceOp::Follow));
        match followed.await? {
            ServiceReply::Follow(b) => Ok(Response::new(Box::pin(GrpcStream(b)))),
            _ => Err(unexpected()),
        }
    }

    type FollowAckedStream =
        Pin<Box<dyn Stream<Item = Result<proto::ReplicationBatch, Status>> + Send>>;

    /// `Replication.FollowAcked` (D335 PR 1): `Follow` for a cluster partition's map
    /// replica, whose acknowledgements on the request stream go to the partition's
    /// [`ReplicaAcks`](super::acks::ReplicaAcks) while the call lasts.
    async fn follow_acked(
        &self,
        req: Request<tonic::Streaming<proto::FollowMessage>>,
    ) -> Result<Response<Self::FollowAckedStream>, Status> {
        use proto::follow_message::Kind;
        let Some(cluster) = self.0.router.cluster.as_ref() else {
            return Err(ApiError::failed_precondition(
                "Replication.FollowAcked is for a cluster partition's replica copies: a \
                 single node's replica uses Follow",
            )
            .to_status());
        };
        #[cfg(test)]
        if super::cluster::OLD_FOLLOW
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|d| cluster.node.dir().starts_with(d))
        {
            return Err(Status::unimplemented("the test's older build"));
        }
        let header = req
            .metadata()
            .get(super::cluster::NODE_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let (metadata, extensions, mut inbound) = req.into_parts();
        let start = match inbound.message().await? {
            Some(proto::FollowMessage {
                kind: Some(Kind::Start(start)),
            }) => start,
            _ => {
                // Never InvalidArgument: a replica takes that as another database's token,
                // and bootstraps again.
                return Err(ApiError::failed_precondition(
                    "Replication.FollowAcked's first message must be a FollowStart",
                )
                .to_status());
            }
        };
        // The node a cluster member claims (the cluster token proves membership, not which
        // node: feed1248's A4), the same in both places.
        if header.as_deref() != Some(start.node.as_str()) {
            return Err(ApiError::failed_precondition(format!(
                "FollowStart's node {:?} isn't the request's node id {:?}",
                start.node,
                header.unwrap_or_default()
            ))
            .to_status());
        }
        // The partition's table, before and after the follow opens: a reopen in between
        // (another `Partition`, so another table) refuses the call, retry-safe, rather than
        // count this stream against a partition it may not be reading (feed1248's 3 on
        // #2236). The parts map is swapped whole, so equal tables mean the same open.
        let table = || {
            cluster
                .parts
                .load()
                .get(&start.partition)
                .and_then(|s| s.part_opt().map(|p| Arc::clone(&p.acks)))
        };
        let before = table();
        let req = Request::from_parts(metadata, extensions, start.request.unwrap_or_default());
        let followed = super::router::FOLLOWER.scope(
            Some(start.node.clone()),
            super::router::FOLLOW_PARTITION.scope(
                Some(start.partition.clone()),
                call(&self.0, req, ServiceOp::Follow),
            ),
        );
        let batches = match followed.await? {
            ServiceReply::Follow(b) => b,
            _ => return Err(unexpected()),
        };
        // The partition that answered: `FOLLOW_PARTITION` kept the call to it.
        let acks = match (before, table()) {
            (Some(b), Some(a)) if Arc::ptr_eq(&a, &b) => a,
            _ => {
                return Err(ApiError::partition_unavailable(format!(
                    "partition {:?} closed or reopened here as its follow began",
                    start.partition
                ))
                .to_status());
            }
        };
        let attached = acks.attach(&start.node);
        #[cfg(test)]
        let reader = tokio::spawn(read_acks(inbound, attached, cluster.node.dir().to_owned()));
        #[cfg(not(test))]
        let reader = tokio::spawn(read_acks(inbound, attached));
        Ok(Response::new(Box::pin(Acked {
            batches: GrpcStream(batches),
            reader,
        })))
    }
}

/// A `FollowAcked` stream's acknowledgements, until the replica ends its side (or sends
/// something else than an `Ack`). Its entry in the partition's table goes with `attached`.
async fn read_acks(
    mut inbound: tonic::Streaming<proto::FollowMessage>,
    attached: super::acks::Attached,
    #[cfg(test)] dir: std::path::PathBuf,
) {
    use proto::follow_message::Kind;
    // The test's hold: the latest acknowledgement held back while the owner is listed.
    #[cfg(test)]
    let mut held: Option<super::acks::Pos> = None;
    #[cfg(test)]
    let holding = || {
        super::cluster::HOLD_ACKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|d| dir.starts_with(d))
    };
    loop {
        #[cfg(test)]
        if let Some(p) = held
            && !holding()
        {
            attached.ack(p);
            held = None;
        }
        #[cfg(test)]
        let m = tokio::select! {
            m = inbound.message() => m,
            () = tokio::time::sleep(std::time::Duration::from_millis(50)), if held.is_some() => {
                continue;
            }
        };
        #[cfg(not(test))]
        let m = inbound.message().await;
        let Ok(Some(m)) = m else {
            return;
        };
        match m.kind {
            Some(Kind::Ack(proto::Ack { position: Some(p) })) => {
                let p = super::acks::Pos::from(&p);
                #[cfg(test)]
                if holding() {
                    held = held.max(Some(p));
                    continue;
                }
                attached.ack(p)
            }
            _ => return,
        }
    }
}

/// `FollowAcked`'s reply stream: `Follow`'s batches. Dropped (the call ended), it stops
/// reading acknowledgements, so the replica's stop counting at once.
struct Acked {
    batches: GrpcStream<super::replica::FollowBatches>,
    reader: tokio::task::JoinHandle<()>,
}

impl Stream for Acked {
    type Item = Result<proto::ReplicationBatch, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.batches).poll_next(cx)
    }
}

impl Drop for Acked {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// `Graph` over gRPC (D313 ticket 7).
#[derive(Debug, Clone)]
pub(crate) struct GraphGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::graph_server::Graph for GraphGrpc {
    async fn traverse(
        &self,
        req: Request<proto::TraverseRequest>,
    ) -> Result<Response<proto::TraverseResponse>, Status> {
        unary!(&self.0, req, Traverse, Traverse)
    }
}

/// `Vectors` over gRPC (D313 ticket 7): `Nearest`, and the IVF index with its client-stream
/// `BulkLoad`.
#[derive(Debug, Clone)]
pub(crate) struct VectorsGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::vectors_server::Vectors for VectorsGrpc {
    async fn nearest(
        &self,
        req: Request<proto::NearestRequest>,
    ) -> Result<Response<proto::NearestResponse>, Status> {
        unary!(&self.0, req, Nearest, Nearest)
    }

    async fn create_index(
        &self,
        req: Request<proto::CreateVectorIndexRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, CreateIndex, Empty)
    }

    async fn insert(
        &self,
        req: Request<proto::VectorInsertRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, VectorInsert, Empty)
    }

    async fn delete(
        &self,
        req: Request<proto::VectorDeleteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, VectorDelete, Empty)
    }

    async fn search(
        &self,
        req: Request<proto::VectorSearchRequest>,
    ) -> Result<Response<proto::VectorSearchResponse>, Status> {
        unary!(&self.0, req, VectorSearch, VectorSearch)
    }

    async fn bulk_load(
        &self,
        mut req: Request<tonic::Streaming<proto::VectorBulkLoadRequest>>,
    ) -> Result<Response<proto::Empty>, Status> {
        let shared = &self.0;
        let caller = caller_of(shared, &mut req)?;
        let first_budget = req.extensions().get::<Prepaid>().and_then(Prepaid::take);
        let meter = req.extensions().get::<Meter>().cloned().unwrap_or_default();
        let limits = shared.limits;
        let deadline = grpc_timeout(req.metadata().as_ref());
        let wait = deadline.map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
        let started = Instant::now();
        let mut stream = req.into_inner();
        let Some(first) = stream.message().await? else {
            return Err(
                ApiError::invalid("a BulkLoad's first message names the index").to_status(),
            );
        };
        let (tx, input) = tokio::sync::mpsc::channel(1);
        let op = ServiceOp::VectorBulkLoad(super::bulk::BulkLoadOp {
            index: first.index.clone(),
            expected: first.expected,
            input,
            wait,
            siblings: 1,
        });
        let rpc = op.index();
        let pump = super::bulk::pump(tx, (first, first_budget), GrpcSource { stream, meter });
        let dispatch = shared.router.dispatch(op, &caller, None);
        tokio::pin!(pump, dispatch);
        // The loader ends the call; a pump that ends first (the stream did) leaves it to
        // finish.
        let reply = tokio::select! {
            r = &mut dispatch => r,
            () = &mut pump => dispatch.await,
        };
        shared.count(rpc, started, &reply);
        match reply? {
            ServiceReply::Empty(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn verify(
        &self,
        req: Request<proto::VectorVerifyRequest>,
    ) -> Result<Response<proto::VectorVerifyResponse>, Status> {
        unary!(&self.0, req, VectorVerify, VectorVerify)
    }
}

/// What [`prepay`] reserved for a client stream: its first message's share, and the meter
/// its later messages' shares arrive on. For a handler outside this module
/// (`Cluster.ForwardBulkLoad`, #1722 PR 3).
pub(crate) fn stream_budget<T>(req: &Request<T>) -> (Option<OwnedSemaphorePermit>, Meter) {
    (
        req.extensions().get::<Prepaid>().and_then(Prepaid::take),
        req.extensions().get::<Meter>().cloned().unwrap_or_default(),
    )
}

/// A forwarded BulkLoad part's later messages (#1722 PR 3), each with the reservation
/// `Metered` made for it: their `load`. A message without one is an empty batch (the
/// sending node's keep-alive while another partition holds it up).
pub(crate) struct ForwardSource {
    pub(crate) stream: tonic::Streaming<proto::ForwardBulkLoadRequest>,
    pub(crate) meter: Meter,
}

impl super::bulk::Source for ForwardSource {
    async fn next(
        &mut self,
    ) -> Result<Option<(proto::VectorBulkLoadRequest, Option<OwnedSemaphorePermit>)>, ApiError>
    {
        match self.stream.message().await {
            Ok(Some(m)) => Ok(Some((
                m.load.unwrap_or_default(),
                self.meter.lock().0.pop_front(),
            ))),
            Ok(None) => Ok(None),
            Err(status) => Err(self.meter.lock().1.take().unwrap_or_else(|| {
                ApiError::invalid(format!(
                    "the forwarded load's stream failed: {}",
                    status.message()
                ))
            })),
        }
    }
}

/// A BulkLoad's later messages, each with the reservation `Metered` made for it.
struct GrpcSource {
    stream: tonic::Streaming<proto::VectorBulkLoadRequest>,
    meter: Meter,
}

impl super::bulk::Source for GrpcSource {
    async fn next(
        &mut self,
    ) -> Result<Option<(proto::VectorBulkLoadRequest, Option<OwnedSemaphorePermit>)>, ApiError>
    {
        match self.stream.message().await {
            Ok(Some(m)) => Ok(Some((m, self.meter.lock().0.pop_front()))),
            Ok(None) => Ok(None),
            Err(status) => Err(self.meter.lock().1.take().unwrap_or_else(|| {
                ApiError::invalid(format!("the load's stream failed: {}", status.message()))
            })),
        }
    }
}

/// `Text` over gRPC (D313 ticket 7c): the BM25 index (D309, ICR 0105).
#[derive(Debug, Clone)]
pub(crate) struct TextGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::text_server::Text for TextGrpc {
    async fn create_index(
        &self,
        req: Request<proto::CreateTextIndexRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, TextCreateIndex, Empty)
    }

    async fn put(
        &self,
        req: Request<proto::TextPutRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, TextPut, Empty)
    }

    async fn delete(
        &self,
        req: Request<proto::TextDeleteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, TextDelete, Empty)
    }

    async fn search(
        &self,
        req: Request<proto::TextSearchRequest>,
    ) -> Result<Response<proto::TextSearchResponse>, Status> {
        unary!(&self.0, req, TextSearch, TextSearch)
    }

    async fn rebuild(
        &self,
        req: Request<proto::TextIndexRequest>,
    ) -> Result<Response<proto::TextRebuildResponse>, Status> {
        unary!(&self.0, req, TextRebuild, TextRebuild)
    }

    async fn verify(
        &self,
        req: Request<proto::TextIndexRequest>,
    ) -> Result<Response<proto::TextVerifyResponse>, Status> {
        unary!(&self.0, req, TextVerify, TextVerify)
    }
}

/// `Memory` over gRPC (D313 ticket 8a): the server's one memory store.
#[derive(Debug, Clone)]
pub(crate) struct MemoryGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::memory_server::Memory for MemoryGrpc {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<proto::MemoryEvent, Status>> + Send>>;

    async fn remember(
        &self,
        req: Request<proto::RememberRequest>,
    ) -> Result<Response<proto::RememberResponse>, Status> {
        unary!(&self.0, req, Remember, Remember)
    }

    async fn recall(
        &self,
        req: Request<proto::RecallRequest>,
    ) -> Result<Response<proto::RecallResponse>, Status> {
        unary!(&self.0, req, Recall, Recall)
    }

    async fn mark_used(
        &self,
        req: Request<proto::MarkUsedRequest>,
    ) -> Result<Response<proto::MarkUsedResponse>, Status> {
        unary!(&self.0, req, MarkUsed, MarkUsed)
    }

    async fn supersede(
        &self,
        req: Request<proto::SupersedeRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Supersede, Empty)
    }

    async fn link(
        &self,
        req: Request<proto::LinkRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Link, Empty)
    }

    async fn forget(
        &self,
        req: Request<proto::ForgetRequest>,
    ) -> Result<Response<proto::ForgetResponse>, Status> {
        unary!(&self.0, req, Forget, Forget)
    }

    async fn watch(
        &self,
        req: Request<proto::MemoryWatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        match call(&self.0, req, ServiceOp::MemoryWatch).await? {
            ServiceReply::MemoryWatch(e) => Ok(Response::new(Box::pin(GrpcStream(e)))),
            _ => Err(unexpected()),
        }
    }
}

/// `Schema` over gRPC (D313 ticket 6).
#[derive(Debug, Clone)]
pub(crate) struct SchemaGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::schema_server::Schema for SchemaGrpc {
    async fn list_tables(
        &self,
        req: Request<proto::ListTablesRequest>,
    ) -> Result<Response<proto::ListTablesResponse>, Status> {
        unary!(&self.0, req, ListTables, Tables)
    }

    async fn create_table(
        &self,
        req: Request<proto::CreateTableRequest>,
    ) -> Result<Response<proto::TableResponse>, Status> {
        unary!(&self.0, req, CreateTable, Table)
    }

    async fn drop_table(
        &self,
        req: Request<proto::DropTableRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, DropTable, Empty)
    }

    async fn describe_table(
        &self,
        req: Request<proto::DescribeTableRequest>,
    ) -> Result<Response<proto::TableResponse>, Status> {
        unary!(&self.0, req, DescribeTable, Table)
    }
}

/// `Snapshots` over gRPC (D313 §3).
#[derive(Debug, Clone)]
pub(crate) struct SnapshotGrpc(pub(crate) Arc<Shared>);

#[tonic::async_trait]
impl proto::snapshots_server::Snapshots for SnapshotGrpc {
    async fn open(
        &self,
        req: Request<proto::OpenSnapshotRequest>,
    ) -> Result<Response<proto::OpenSnapshotResponse>, Status> {
        unary!(&self.0, req, OpenSnapshot, OpenSnapshot)
    }

    async fn release(
        &self,
        req: Request<proto::ReleaseSnapshotRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, ReleaseSnapshot, Empty)
    }
}

fn unexpected() -> Status {
    Status::internal("the service answered with another RPC's reply")
}

#[tonic::async_trait]
impl Data for DataGrpc {
    async fn get(
        &self,
        req: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        respond(self.call(req, ServiceOp::Get).await?, |r| match r {
            ServiceReply::Get(r) => Some(r),
            _ => None,
        })
    }

    async fn multi_get(
        &self,
        req: Request<proto::MultiGetRequest>,
    ) -> Result<Response<proto::MultiGetResponse>, Status> {
        respond(self.call(req, ServiceOp::MultiGet).await?, |r| match r {
            ServiceReply::MultiGet(r) => Some(r),
            _ => None,
        })
    }

    async fn read_row(
        &self,
        req: Request<proto::ReadRowRequest>,
    ) -> Result<Response<proto::ReadRowResponse>, Status> {
        respond(self.call(req, ServiceOp::ReadRow).await?, |r| match r {
            ServiceReply::ReadRow(r) => Some(r),
            _ => None,
        })
    }

    type ScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    async fn scan(
        &self,
        req: Request<proto::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        match self.call(req, ServiceOp::Scan).await? {
            ServiceReply::Scan(pages) => Ok(Response::new(Box::pin(GrpcStream(pages)))),
            _ => Err(unexpected()),
        }
    }

    async fn mutate(
        &self,
        req: Request<proto::MutateRequest>,
    ) -> Result<Response<proto::MutateResponse>, Status> {
        match self.call(req, ServiceOp::Mutate).await? {
            ServiceReply::Mutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn check_and_mutate(
        &self,
        req: Request<proto::CheckAndMutateRequest>,
    ) -> Result<Response<proto::CheckAndMutateResponse>, Status> {
        match self.call(req, ServiceOp::CheckAndMutate).await? {
            ServiceReply::CheckAndMutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn batch(
        &self,
        req: Request<proto::BatchRequest>,
    ) -> Result<Response<proto::MutateResponse>, Status> {
        match self.call(req, ServiceOp::Batch).await? {
            ServiceReply::Mutate(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn mutate_returning(
        &self,
        req: Request<proto::MutateRequest>,
    ) -> Result<Response<proto::MutateReturningResponse>, Status> {
        match self.call(req, ServiceOp::MutateReturning).await? {
            ServiceReply::MutateReturning(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_serves_subscribe_and_refuses_any_other_path() {
        // `Feed` is mounted on every path under its service, and routed by hand: its
        // `refuses` is all that keeps another method's request from being read as a
        // `Subscribe` (feed1248's S1 on #1923).
        assert!(FeedGrpc::refuses("/pigeonhole.v1.Feed/Subscribe").is_none());
        for path in ["/pigeonhole.v1.Feed/Other", "/pigeonhole.v1.Feed/", ""] {
            let refused = FeedGrpc::refuses(path).unwrap_or_else(|| panic!("{path:?}"));
            assert_eq!(refused.code(), tonic::Code::Unimplemented, "{path:?}");
            assert_eq!(refused.message(), "no such method in pigeonhole.v1.Feed");
        }
    }
}
