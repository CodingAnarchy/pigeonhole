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
use tonic::{Request, Response, Status};

use super::Shared;
use super::errors::ApiError;
use super::proto::{self, data_server::Data};
use super::service::{ScanPages, ServiceOp, ServiceReply};

/// The budget `prepay` reserved for a request, for its handler to take. Extensions must be
/// `Clone`; the permit is taken once.
#[derive(Clone, Default)]
struct Prepaid(Arc<Mutex<Option<OwnedSemaphorePermit>>>);

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
/// `max_decoding_message_size`, reserved instead. TODO(`Vectors.BulkLoad`'s ticket): a
/// client-streaming RPC needs a reservation per message; this reserves only the first.
pub(crate) async fn prepay(
    State(shared): State<Arc<Shared>>,
    req: HttpRequest,
    next: Next,
) -> HttpResponse {
    let (parts, body) = req.into_parts();
    let mut data = body.into_data_stream();
    let mut head = Vec::new();
    while head.len() < 5 {
        match std::future::poll_fn(|cx| Pin::new(&mut data).poll_next(cx)).await {
            Some(Ok(chunk)) => head.extend_from_slice(&chunk),
            Some(Err(_)) => {
                return status(ApiError::invalid("the request body broke off"));
            }
            // A body shorter than a header: tonic answers that.
            None => break,
        }
    }
    let length = head
        .get(1..5)
        .map_or(0, |b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize);
    let limits = shared.limits;
    let deadline = grpc_timeout(&parts.headers);
    let wait = deadline.map_or(limits.request_timeout, |d| d.min(limits.request_timeout));
    let bytes = length.min(limits.max_request_bytes);
    let permit = match shared.router.service.reserve_within(bytes, wait).await {
        Ok(p) => p,
        // The client's own deadline ran out first.
        Err(_) if deadline.is_some_and(|d| d <= limits.request_timeout) => {
            return status(ApiError::deadline_exceeded());
        }
        Err(e) => return status(e),
    };
    let mut req = HttpRequest::from_parts(
        parts,
        Body::from_stream(Rest {
            head: Some(Bytes::from(head)),
            rest: data,
        }),
    );
    req.extensions_mut()
        .insert(Prepaid(Arc::new(Mutex::new(Some(permit)))));
    next.run(req).await
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
    let v = headers.get("grpc-timeout")?.to_str().ok()?;
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

/// A scan's pages as gRPC messages.
struct GrpcPages(ScanPages);

impl Stream for GrpcPages {
    type Item = Result<proto::ScanPage, Status>;

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
    let op = op(req.into_inner());
    let rpc = op.index();
    let reply = shared.router.dispatch(op, &caller, prepaid).await;
    shared.count(rpc, started, &reply);
    Ok(reply?)
}

/// A unary RPC: the call, and its reply's variant.
macro_rules! unary {
    ($shared:expr, $req:expr, $op:ident, $reply:ident) => {
        match call($shared, $req, ServiceOp::$op).await? {
            ServiceReply::$reply(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    };
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
#[derive(Debug, Clone)]
pub(crate) struct FeedGrpc(pub(crate) Arc<Shared>);

struct GrpcEvents(super::feed::FeedEvents);

impl Stream for GrpcEvents {
    type Item = Result<proto::FeedEvent, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map(|item| item.map(|r| r.map_err(|e| e.to_status())))
    }
}

#[tonic::async_trait]
impl proto::feed_server::Feed for FeedGrpc {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<proto::FeedEvent, Status>> + Send>>;

    async fn subscribe(
        &self,
        req: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        match call(&self.0, req, ServiceOp::Subscribe).await? {
            ServiceReply::Feed(events) => Ok(Response::new(Box::pin(GrpcEvents(events)))),
            _ => Err(unexpected()),
        }
    }
}

/// `Admin` over gRPC (D313 ticket 6).
#[derive(Debug, Clone)]
pub(crate) struct AdminGrpc(pub(crate) Arc<Shared>);

struct GrpcChunks(super::backup::Download);

impl Stream for GrpcChunks {
    type Item = Result<proto::BackupChunk, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0)
            .poll_next(cx)
            .map(|item| item.map(|r| r.map_err(|e| e.to_status())))
    }
}

#[tonic::async_trait]
impl proto::admin_server::Admin for AdminGrpc {
    /// Liveness, with no token: whether the database is open and not failed.
    async fn health(
        &self,
        _req: Request<proto::Empty>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        let ok = !self.0.router.service.db.is_failed();
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

    async fn flush(&self, req: Request<proto::Empty>) -> Result<Response<proto::Empty>, Status> {
        unary!(&self.0, req, Flush, Empty)
    }

    async fn compact(&self, req: Request<proto::Empty>) -> Result<Response<proto::Empty>, Status> {
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
            ServiceReply::Download(d) => Ok(Response::new(Box::pin(GrpcChunks(d)))),
            _ => Err(unexpected()),
        }
    }

    async fn metrics(
        &self,
        req: Request<proto::Empty>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        unary!(&self.0, req, Metrics, Metrics)
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
        match self.call(req, ServiceOp::Get).await? {
            ServiceReply::Get(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn multi_get(
        &self,
        req: Request<proto::MultiGetRequest>,
    ) -> Result<Response<proto::MultiGetResponse>, Status> {
        match self.call(req, ServiceOp::MultiGet).await? {
            ServiceReply::MultiGet(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    async fn read_row(
        &self,
        req: Request<proto::ReadRowRequest>,
    ) -> Result<Response<proto::ReadRowResponse>, Status> {
        match self.call(req, ServiceOp::ReadRow).await? {
            ServiceReply::ReadRow(r) => Ok(Response::new(r)),
            _ => Err(unexpected()),
        }
    }

    type ScanStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<proto::ScanPage, Status>> + Send>>;

    async fn scan(
        &self,
        req: Request<proto::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        match self.call(req, ServiceOp::Scan).await? {
            ServiceReply::Scan(pages) => Ok(Response::new(Box::pin(GrpcPages(pages)))),
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
