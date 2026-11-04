//! HTTP/JSON (ICR 0112): `POST /v1/{Service}/{Method}` with the request message as proto3
//! JSON, the reply likewise; `GET /v1/health` without a token; `GET /metrics` (read scope).
//! The gRPC services share the router, on their own paths, so one port serves both.

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
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
    let shed = Arc::clone(&shared);
    let limited = axum::Router::new()
        .route("/v1/{service}/{method}", post(call))
        .route_service("/pigeonhole.v1.Data/{*method}", data)
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
    let text = shared.metrics.render(&shared.router.service.db);
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
    let op = match decode(&service, &method, &body) {
        Ok(op) => op,
        Err(e) => return e.into_response(),
    };
    let rpc = op.index();
    let reply = shared.router.dispatch(op, &caller, Some(budget)).await;
    shared.count(rpc, &reply);
    match reply {
        Ok(ServiceReply::Get(r)) => json(&r),
        Ok(ServiceReply::MultiGet(r)) => json(&r),
        Ok(ServiceReply::ReadRow(r)) => json(&r),
        Ok(ServiceReply::Mutate(r)) => json(&r),
        Ok(ServiceReply::CheckAndMutate(r)) => json(&r),
        Ok(ServiceReply::MutateReturning(r)) => json(&r),
        Err(e) => e.into_response(),
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
        ("Data", "Scan") => return Err(ApiError::unimplemented("Data.Scan")),
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
