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

use super::Shared;
use super::errors::ApiError;
use super::feed::Out;
use super::proto::{self, data_server::Data};
use super::replica::ReadDeadline;
use super::service::{ServiceOp, ServiceReply};

/// The budget `prepay` reserved for a request, for its handler to take. Extensions must be
/// `Clone`; the permit is taken once.
#[derive(Clone, Default)]
struct Prepaid(Arc<Mutex<Option<OwnedSemaphorePermit>>>);

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
    if let Err(e) = shared.caller(token) {
        return status(e);
    }
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
    // `--max-request-bytes` is left for tonic to refuse from its header.
    if length <= limits.max_request_bytes
        && let Err(e) = read_to(&mut data, &mut head, 5 + length, until).await
    {
        return status(e);
    }
    let rest = Rest {
        head: Some(Bytes::from(head)),
        rest: data,
    };
    let meter = Meter::default();
    let body = if parts.uri.path() == BULK_LOAD {
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

/// The reservations [`Metered`] made for a stream's later messages, in order, for its
/// handler to take one per message; or the reservation that failed.
#[derive(Clone, Default)]
struct Meter(
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
fn grpc_timeout(headers: &axum::http::HeaderMap) -> Option<Duration> {
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

/// One gRPC call through the router, for every service.
async fn call<T>(
    shared: &Shared,
    req: Request<T>,
    op: impl FnOnce(T) -> ServiceOp,
) -> Result<ServiceReply, Status> {
    let header = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok());
    let caller = shared.caller(header)?;
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
}

/// `Feed` over gRPC (D313 R2): the fan-out's events, an error as the stream's final status.
///
/// Routed by hand rather than through the generated `FeedServer`, for its codec: a ring entry
/// sent to every client with no prefix is written as the bytes it was encoded to once, not
/// built and encoded again per subscriber (perf287's profile on #1536). The wire format is
/// the generated one; `Feed` has the one RPC.
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

/// `Feed.Subscribe`'s codec: requests are decoded as the generated codec does, and events
/// written by [`Out::encode`].
struct FeedCodec;

impl tonic::codec::Codec for FeedCodec {
    type Encode = Out;
    type Decode = proto::SubscribeRequest;
    type Encoder = OutEncoder;
    type Decoder = tonic_prost::ProstDecoder<proto::SubscribeRequest>;

    fn encoder(&mut self) -> Self::Encoder {
        OutEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstDecoder::default()
    }
}

pub(crate) struct OutEncoder;

impl tonic::codec::Encoder for OutEncoder {
    type Item = Out;
    type Error = Status;

    fn encode(&mut self, item: Out, dst: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Status> {
        // The buffer grows as needed: as for the generated codec, encoding can't run out.
        item.encode(dst)
            .map_err(|e| Status::internal(format!("encoding a feed event: {e}")))
    }
}

struct SubscribeSvc(Arc<Shared>);

impl tonic::server::ServerStreamingService<proto::SubscribeRequest> for SubscribeSvc {
    type Response = Out;
    type ResponseStream = GrpcEvents;
    type Future = tonic::codegen::BoxFuture<Response<GrpcEvents>, Status>;

    fn call(&mut self, req: Request<proto::SubscribeRequest>) -> Self::Future {
        let shared = Arc::clone(&self.0);
        Box::pin(async move {
            match call(&shared, req, ServiceOp::Subscribe).await? {
                ServiceReply::Feed(events) => Ok(Response::new(GrpcEvents(events))),
                _ => Err(unexpected()),
            }
        })
    }
}

impl<B> tower::Service<http::Request<B>> for FeedGrpc
where
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
        if req.uri().path() != "/pigeonhole.v1.Feed/Subscribe" {
            return Box::pin(async {
                Ok(Status::unimplemented("no such method in pigeonhole.v1.Feed").into_http())
            });
        }
        let svc = SubscribeSvc(Arc::clone(&self.shared));
        let max = self.max_request_bytes;
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(FeedCodec)
                .apply_max_message_size_config(Some(max), Some(usize::MAX));
            Ok(grpc.server_streaming(svc, req).await)
        })
    }
}

impl tonic::server::NamedService for FeedGrpc {
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
        req: Request<tonic::Streaming<proto::PartitionUploadChunk>>,
    ) -> Result<Response<proto::PartitionUploadReply>, Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let caller = self.0.caller(header)?;
        let reply = self
            .0
            .router
            .partition_upload(&caller, req.into_inner())
            .await?;
        Ok(Response::new(reply))
    }

    async fn partition_upload_status(
        &self,
        req: Request<proto::PartitionUploadStatusRequest>,
    ) -> Result<Response<proto::PartitionUploadStatusReply>, Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let caller = self.0.caller(header)?;
        let reply = self
            .0
            .router
            .partition_upload_status(&caller, &req.get_ref().upload_id)?;
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
        match call(&self.0, req, ServiceOp::Follow).await? {
            ServiceReply::Follow(b) => Ok(Response::new(Box::pin(GrpcStream(b)))),
            _ => Err(unexpected()),
        }
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
        req: Request<tonic::Streaming<proto::VectorBulkLoadRequest>>,
    ) -> Result<Response<proto::Empty>, Status> {
        let shared = &self.0;
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        let caller = shared.caller(header)?;
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
