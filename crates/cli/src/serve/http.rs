//! HTTP/JSON (ICR 0112): `POST /v1/{Service}/{Method}` with the request message as proto3
//! JSON, the reply likewise; `GET /v1/health` without a token; `GET /metrics` (read scope).
//! A server stream (`Data.Scan`) answers as server-sent events for `Accept:
//! text/event-stream`, otherwise as NDJSON, and a failure ends it with a final error message.
//! The gRPC services share the router, on their own paths, so one port serves both.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_core::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::Shared;
use super::auth::Scope;
use super::errors::ApiError;
use super::grpc::DataGrpc;
use super::proto;
use super::service::{ServiceOp, ServiceReply};

/// The whole router: gRPC and HTTP/JSON behind the concurrency limit and load shedding,
/// and beside them, outside the limit, health and metrics: a probe or a scrape under
/// overload must still answer, or a load balancer takes the node out exactly when it's
/// busiest (perf287's F4 on #1501).
pub(crate) fn router(shared: Arc<Shared>) -> axum::Router {
    let limits = shared.limits;
    let data = proto::data_server::DataServer::new(DataGrpc(Arc::clone(&shared)))
        .max_decoding_message_size(limits.max_request_bytes)
        .max_encoding_message_size(usize::MAX);
    let txns = proto::transactions_server::TransactionsServer::new(super::grpc::TxnGrpc(
        Arc::clone(&shared),
    ))
    .max_decoding_message_size(limits.max_request_bytes)
    .max_encoding_message_size(usize::MAX);
    let feeds = proto::feed_server::FeedServer::new(super::grpc::FeedGrpc(Arc::clone(&shared)))
        .max_decoding_message_size(limits.max_request_bytes)
        .max_encoding_message_size(usize::MAX);
    let snapshots = proto::snapshots_server::SnapshotsServer::new(super::grpc::SnapshotGrpc(
        Arc::clone(&shared),
    ))
    .max_decoding_message_size(limits.max_request_bytes)
    .max_encoding_message_size(usize::MAX);
    let prepay = || axum::middleware::from_fn_with_state(Arc::clone(&shared), super::grpc::prepay);
    let shed = Arc::clone(&shared);
    let limited = axum::Router::new()
        .route("/v1/{service}/{method}", post(call))
        .route_service(
            "/pigeonhole.v1.Data/{*method}",
            tower::ServiceBuilder::new().layer(prepay()).service(data),
        )
        .route_service(
            "/pigeonhole.v1.Transactions/{*method}",
            tower::ServiceBuilder::new().layer(prepay()).service(txns),
        )
        .route_service(
            "/pigeonhole.v1.Feed/{*method}",
            tower::ServiceBuilder::new().layer(prepay()).service(feeds),
        )
        .route_service(
            "/pigeonhole.v1.Snapshots/{*method}",
            tower::ServiceBuilder::new()
                .layer(prepay())
                .service(snapshots),
        )
        .with_state(Arc::clone(&shared))
        // Overload is refused at once, never queued (perf287's R3): `ConcurrencyLimit`
        // alone would queue callers in `poll_ready`.
        .layer(
            tower::ServiceBuilder::new()
                .layer(axum::error_handling::HandleErrorLayer::new(
                    move |headers: HeaderMap, _: tower::BoxError| {
                        let shed = Arc::clone(&shed);
                        async move {
                            shed.metrics
                                .shed
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            error_response(
                                &headers,
                                ApiError::overloaded(
                                    "the server is at its concurrent request limit",
                                ),
                            )
                        }
                    },
                ))
                .load_shed()
                .concurrency_limit(limits.max_concurrent),
        );
    axum::Router::new()
        .route("/v1/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(shared)
        .merge(limited)
}

/// An error in the protocol the request spoke: a gRPC status for gRPC, JSON otherwise.
fn error_response(headers: &HeaderMap, e: ApiError) -> Response {
    let grpc = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/grpc"));
    if grpc {
        let (parts, body) = e.to_status().into_http::<tonic::body::Body>().into_parts();
        return Response::from_parts(parts, Body::new(body));
    }
    e.into_response()
}

async fn health(State(shared): State<Arc<Shared>>) -> Response {
    // Liveness only, with no token: whether the database is open and not failed.
    let ok = !shared.router.service.db.is_failed();
    let reply = proto::HealthResponse {
        status: if ok { "ok" } else { "failed" }.into(),
    };
    let mut resp = json(&reply);
    if !ok {
        *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    }
    resp
}

async fn metrics(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let caller = match shared.caller(auth_header(&headers)) {
        Ok(c) => c,
        Err(_) => return ApiError::unauthenticated().into_response(),
    };
    if caller.scope < Scope::Read {
        return ApiError::permission_denied("read metrics").into_response();
    }
    let mut text = shared.metrics.render(&shared.router.service.db);
    super::service::render_sessions(&mut text, &shared.router.service.sessions);
    super::service::render_feeds(&mut text, &shared.router.service.feeds);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response()
}

fn auth_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
}

/// `POST /v1/{service}/{method}`.
async fn call(
    State(shared): State<Arc<Shared>>,
    Path((service, method)): Path<(String, String)>,
    req: Request,
) -> Response {
    let headers = req.headers().clone();
    let caller = match shared.caller(auth_header(&headers)) {
        Ok(c) => c,
        Err(_) => return ApiError::unauthenticated().into_response(),
    };
    // The in-flight budget is reserved from `Content-Length` before any of the body is
    // read (perf287's F2 on #1501), so waiting requests hold no bodies. A body without a
    // length (chunked) is refused; no header and no `Transfer-Encoding` is an empty body.
    let limit = shared.limits.max_request_bytes;
    let length = match content_length(&headers) {
        Ok(n) => n,
        Err(e) => return e.into_response(),
    };
    if length > limit {
        return ApiError::request_too_large(limit).into_response();
    }
    let budget = match shared.router.service.reserve(length).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let body = match axum::body::to_bytes(req.into_body(), length).await {
        Ok(b) => b,
        Err(_) => {
            return ApiError::invalid("the body is longer than its Content-Length").into_response();
        }
    };
    let started = Instant::now();
    let mut op = match decode(&service, &method, &body) {
        Ok(op) => op,
        Err(e) => return e.into_response(),
    };
    // An SSE client reconnects with `Last-Event-ID`, the last event's `id:` (its token): it
    // takes precedence over the body's `resume` (ICR 0112).
    if let ServiceOp::Subscribe(r) = &mut op
        && let Some(id) = headers.get("last-event-id").and_then(|v| v.to_str().ok())
    {
        match serde_json::from_value::<proto::SubscribeRequest>(serde_json::json!({ "resume": id }))
        {
            Ok(parsed) => r.start = parsed.start,
            Err(_) => return ApiError::invalid("Last-Event-ID isn't a feed token").into_response(),
        }
    }
    let rpc = op.index();
    let reply = shared.router.dispatch(op, &caller, Some(budget)).await;
    shared.count(rpc, started, &reply);
    match reply {
        Ok(ServiceReply::Get(r)) => json(&r),
        Ok(ServiceReply::MultiGet(r)) => json(&r),
        Ok(ServiceReply::ReadRow(r)) => json(&r),
        Ok(ServiceReply::Mutate(r)) => json(&r),
        Ok(ServiceReply::CheckAndMutate(r)) => json(&r),
        Ok(ServiceReply::MutateReturning(r)) => json(&r),
        Ok(ServiceReply::Scan(pages)) => stream(&headers, pages),
        Ok(ServiceReply::Feed(events)) => stream(&headers, events),
        Ok(ServiceReply::Begin(r)) => json(&r),
        Ok(ServiceReply::Commit(r)) => json(&r),
        Ok(ServiceReply::OpenSnapshot(r)) => json(&r),
        Ok(ServiceReply::Empty(r)) => json(&r),
        Err(e) => e.into_response(),
    }
}

/// A message of a server stream: its JSON, and its SSE `id:` if it has one.
trait Event: Serialize {
    fn sse_id(&self) -> Option<String> {
        None
    }
}

impl Event for proto::ScanPage {}

impl Event for proto::FeedEvent {
    /// The event's token, as its JSON gives it, so `Last-Event-ID` resumes (ICR 0112).
    fn sse_id(&self) -> Option<String> {
        Some(base64(&self.token))
    }
}

/// Standard base64, padded: the proto3 JSON mapping's bytes.
fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            out.push(if i <= c.len() {
                char::from(A[(n >> shift) as usize & 63])
            } else {
                '='
            });
        }
    }
    out
}

/// How long an SSE stream may be silent before a keepalive comment.
const SSE_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(15);

/// A server stream's messages as server-sent events or NDJSON (ICR 0112).
fn stream<S, T>(headers: &HeaderMap, items: S) -> Response
where
    S: Stream<Item = Result<T, ApiError>> + Send + Unpin + 'static,
    T: Event + Send + 'static,
{
    let sse = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/event-stream"));
    let content_type = if sse {
        "text/event-stream"
    } else {
        "application/x-ndjson"
    };
    let mut resp = Body::from_stream(Lines {
        items,
        sse,
        keepalive: Box::pin(tokio::time::sleep(SSE_KEEPALIVE)),
    })
    .into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    resp
}

/// Each message as one SSE event (`data:` its JSON, and `id:` for a feed event) or one
/// NDJSON line; an error as an `event: error` or a `{"error": ErrorDetail}` line, after which
/// the stream has ended. A silent SSE stream sends a keepalive comment every
/// `SSE_KEEPALIVE`, so proxies keep it open.
struct Lines<S> {
    items: S,
    sse: bool,
    keepalive: Pin<Box<tokio::time::Sleep>>,
}

impl<S, T> Stream for Lines<S>
where
    S: Stream<Item = Result<T, ApiError>> + Unpin,
    T: Event,
{
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let sse = self.sse;
        let polled = Pin::new(&mut self.items).poll_next(cx);
        if polled.is_pending() {
            if sse && self.keepalive.as_mut().poll(cx).is_ready() {
                let next = tokio::time::Instant::now() + SSE_KEEPALIVE;
                self.keepalive.as_mut().reset(next);
                return Poll::Ready(Some(Ok(Bytes::from_static(b": keepalive\n\n"))));
            }
            return Poll::Pending;
        }
        if sse {
            let next = tokio::time::Instant::now() + SSE_KEEPALIVE;
            self.keepalive.as_mut().reset(next);
        }
        polled.map(|item| {
            item.map(|page| {
                let id = page.as_ref().ok().and_then(Event::sse_id);
                let (event, body) = match page {
                    Ok(page) => (None, serde_json::to_vec(&page)),
                    Err(e) => (
                        Some("error"),
                        if sse {
                            serde_json::to_vec(&e.detail())
                        } else {
                            serde_json::to_vec(&serde_json::json!({ "error": e.detail() }))
                        },
                    ),
                };
                // A page that can't be encoded is a server bug; it ends the stream as an
                // error the client can see rather than as a silent cut.
                let body = body.unwrap_or_else(|e| {
                    let e = ApiError::internal(format!("encoding a scan page: {e}"));
                    serde_json::to_vec(&serde_json::json!({ "error": e.detail() }))
                        .unwrap_or_default()
                });
                let mut out = Vec::with_capacity(body.len() + 24);
                if sse {
                    if let Some(id) = id {
                        out.extend_from_slice(b"id: ");
                        out.extend_from_slice(id.as_bytes());
                        out.push(b'\n');
                    }
                    if let Some(event) = event {
                        out.extend_from_slice(b"event: ");
                        out.extend_from_slice(event.as_bytes());
                        out.push(b'\n');
                    }
                    out.extend_from_slice(b"data: ");
                    out.extend_from_slice(&body);
                    out.extend_from_slice(b"\n\n");
                } else {
                    out.extend_from_slice(&body);
                    out.push(b'\n');
                }
                Ok(Bytes::from(out))
            })
        })
    }
}

/// The body's length from `Content-Length`; 0 with neither it nor `Transfer-Encoding`.
fn content_length(headers: &HeaderMap) -> Result<usize, ApiError> {
    match headers.get(header::CONTENT_LENGTH) {
        Some(v) => v
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .ok_or_else(|| ApiError::invalid("Content-Length isn't a number")),
        None if headers.contains_key(header::TRANSFER_ENCODING) => Err(ApiError::invalid(
            "a request body needs a Content-Length (chunked bodies aren't taken)",
        )),
        None => Ok(0),
    }
}

fn decode(service: &str, method: &str, body: &Bytes) -> Result<ServiceOp, ApiError> {
    fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
        let text = if body.is_empty() {
            &b"{}"[..]
        } else {
            &body[..]
        };
        serde_json::from_slice(text).map_err(|e| {
            ApiError::invalid(format!("the request isn't valid JSON for its message: {e}"))
        })
    }
    Ok(match (service, method) {
        ("Data", "Get") => ServiceOp::Get(parse(body)?),
        ("Data", "MultiGet") => ServiceOp::MultiGet(parse(body)?),
        ("Data", "ReadRow") => ServiceOp::ReadRow(parse(body)?),
        ("Data", "Mutate") => ServiceOp::Mutate(parse(body)?),
        ("Data", "CheckAndMutate") => ServiceOp::CheckAndMutate(parse(body)?),
        ("Data", "Batch") => ServiceOp::Batch(parse(body)?),
        ("Data", "MutateReturning") => ServiceOp::MutateReturning(parse(body)?),
        ("Data", "Scan") => ServiceOp::Scan(parse(body)?),
        ("Transactions", "Begin") => ServiceOp::Begin(parse(body)?),
        ("Transactions", "Get") => ServiceOp::TxnGet(parse(body)?),
        ("Transactions", "GetRow") => ServiceOp::TxnGetRow(parse(body)?),
        ("Transactions", "Write") => ServiceOp::TxnWrite(parse(body)?),
        ("Transactions", "Commit") => ServiceOp::Commit(parse(body)?),
        ("Transactions", "Abort") => ServiceOp::Abort(parse(body)?),
        ("Snapshots", "Open") => ServiceOp::OpenSnapshot(parse(body)?),
        ("Snapshots", "Release") => ServiceOp::ReleaseSnapshot(parse(body)?),
        ("Feed", "Subscribe") => ServiceOp::Subscribe(parse(body)?),
        _ => {
            return Err(ApiError::not_found(format!(
                "no RPC /v1/{service}/{method} in this build"
            )));
        }
    })
}

fn json(message: &impl Serialize) -> Response {
    match serde_json::to_vec(message) {
        Ok(body) => {
            let mut resp = (StatusCode::OK, body).into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            resp
        }
        Err(e) => ApiError::internal(format!("encoding the reply: {e}")).into_response(),
    }
}
