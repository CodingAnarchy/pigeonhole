//! Remote MCP: `phdb mcp`'s tools over MCP's Streamable HTTP transport at `/mcp` (D313 §7,
//! ticket 8c-3a; ICR 0112's remote MCP section).
//!
//! - **The same tools and protocol as `phdb mcp`** ([`crate::mcp`]): their database calls go
//!   through [`ServeBackend`], which gets its tables and makes its schema changes through
//!   the service layer's own functions (`Service::table`, `create_with`, `user_tables`). So
//!   the reserved `__phdb_` prefix and the service's table cache apply as on gRPC and HTTP.
//! - **Every tool call passes [`Service::admit`](super::service::Service::admit)** first,
//!   as every RPC does: the caller's scope (read, or write for `write` and `create_table`),
//!   a replica's write refusal. A refusal is the tool's result, with `isError`, so the agent
//!   sees it. A token with read scope isn't offered the write tools.
//! - **The request's bytes** are the POST's: reserved from its `Content-Length` before its
//!   body is read, as for `/v1`, and held until its reply is built. Its tool calls reserve
//!   no more.
//! - **Sessions** (`Mcp-Session-Id`) live in the session table, bound to their token, with
//!   leases of their own. A session answers up to `--mcp-session-requests` requests at once;
//!   its state sits behind small locks of its own, never held across a tool call.
//! - **`MCP-Protocol-Version`:** a request whose header names another version than the
//!   session's gets 400. A request without it is taken as the session's (the spec's
//!   fallback is 2025-03-26, which this server's tools answer the same).
//! - **`notifications/tools/list_changed`** rides on a POST's reply as an SSE stream, when
//!   a call (this session's, or another's: the service's catalog generation) changed the
//!   tools the client last listed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pigeonhole::Pigeonhole;
use serde_json::{Map, Value};
use tokio::sync::Semaphore;

use super::Shared;
use super::auth::{Caller, Scope};
use super::errors::ApiError;
use super::mcp_backend::{ServeBackend, ServeFeed, ServeMemory, failure};
use super::service::Admit;
use crate::mcp::backend::FeedSource;
use crate::mcp::memory::Memory;
use crate::mcp::protocol::{self, Core, Host, Transport};
use crate::mcp::tools::{Outcome, Tools};

/// The session's header (Streamable HTTP, 2025-03-26).
const SESSION: &str = "mcp-session-id";
/// The protocol version's header (2025-06-18).
const VERSION: &str = "mcp-protocol-version";
/// How long a request past its session's `--mcp-session-requests` waits for a place.
const SESSION_WAIT: Duration = Duration::from_secs(1);

/// One `/mcp` session: its tools (with their cursors' cache), its protocol state, and its
/// request cap.
pub(crate) struct McpSession {
    tools: Tools,
    protocol: protocol::Session,
    requests: Arc<Semaphore>,
    /// The service's catalog generation the session last saw.
    catalog: AtomicU64,
}

impl McpSession {
    fn new(shared: &Arc<Shared>, caller: &Caller, db: Pigeonhole) -> Self {
        let service = &shared.router.service;
        let writable = caller.scope.allows(Scope::Write) && !refuses_writes(shared);
        let backend = ServeBackend {
            shared: Arc::downgrade(shared),
            caller: caller.clone(),
            db,
            writable,
            replica: service.replica.is_some(),
        };
        // The memory tools over the served store (8c-3b): `Memory` ops as the session's token,
        // so limited to its namespaces (M4) and, on a cluster, reaching every partition (#1915
        // PR 3b). Listed with `--memory` only: its flags (the embedder, the budgets) are the
        // node's, and a recall across partitions needs them on the node called (S5).
        let memory = shared.router.memory_slot().map(|slot| {
            Memory::served(
                slot.name(),
                Box::new(ServeMemory {
                    backend: backend.clone(),
                    slot: Arc::clone(&slot),
                }),
                writable,
                slot.embeds(),
                shared.limits.mcp.result_bytes,
                slot.max_recall(),
            )
        });
        let tools = Tools::new(Box::new(backend), writable, shared.limits.mcp, memory);
        Self {
            tools: if caller.memory_only() {
                tools.memory_only()
            } else {
                tools
            },
            protocol: protocol::Session::default(),
            requests: Arc::new(Semaphore::new(shared.limits.mcp_session_requests)),
            catalog: AtomicU64::new(service.catalog()),
        }
    }
}

fn refuses_writes(shared: &Shared) -> bool {
    shared
        .router
        .service
        .replica
        .as_ref()
        .is_some_and(|r| r.refuses_writes())
}

/// What one `/mcp` request's messages reach: the session's tools, each call admitted first.
struct Gate<'a> {
    session: &'a McpSession,
    shared: &'a Shared,
    caller: &'a Caller,
    /// The feed a `feed_poll` message's poll reads: opened and waited on by `post` before
    /// its `/mcp` place (simp724's M2).
    feed: std::sync::Mutex<Option<ServeFeed>>,
}

impl Host for Gate<'_> {
    fn memory(&self) -> Option<&Memory> {
        self.session.tools.memory()
    }

    fn list(&self) -> Vec<Value> {
        self.session.tools.list()
    }

    fn names(&self) -> Vec<String> {
        self.session.tools.names()
    }

    fn call(&self, name: &str, args: &Map<String, Value>) -> Option<Outcome> {
        let (what, scope) = match name {
            "list_tables" => ("list_tables", Scope::Read),
            "read_row" => ("read_row", Scope::Read),
            "scan" => ("scan", Scope::Read),
            "traverse" => ("traverse", Scope::Read),
            "nearest" => ("nearest", Scope::Read),
            "feed_poll" => ("feed_poll", Scope::Read),
            "write" => ("write", Scope::Write),
            "create_table" => ("create_table", Scope::Write),
            "memory_recall" => ("memory_recall", Scope::Read),
            "memory_remember" => ("memory_remember", Scope::Write),
            "memory_mark_used" => ("memory_mark_used", Scope::Write),
            "memory_supersede" => ("memory_supersede", Scope::Write),
            "memory_link" => ("memory_link", Scope::Write),
            "memory_forget" => ("memory_forget", Scope::Write),
            // Not a tool here: the protocol answers it.
            _ => return self.session.tools.call(name, args),
        };
        let service = &self.shared.router.service;
        let admit = Admit {
            what,
            scope,
            writes: scope == Scope::Write,
            // The request's bytes are the POST's, reserved before its body was read.
            size: 0,
            memory: what.starts_with("memory_"),
            peer: false,
        };
        // The checks only: the request's bytes are its POST's, already reserved (ai1249's S1).
        if let Err(e) = service.check(&admit, self.caller) {
            return Some(Err(failure(e)));
        }
        // A poll is a subscription, whose op holds its `--max-streams` place (simp724's S2).
        // It never waits here, holding the `/mcp` place: `post` waited for its first commit
        // before taking it (the audit's F7, simp724's M2), so the call takes what's ready.
        if name == "feed_poll" {
            let feed = self
                .feed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .map(|f| Box::new(f) as Box<dyn FeedSource>);
            return Some(self.session.tools.feed_poll_now(args, feed));
        }
        self.session.tools.call(name, args)
    }
}

/// `POST /mcp`: one JSON-RPC message (or a batch) for a session, or an `initialize` that
/// starts one.
pub(crate) async fn post(State(shared): State<Arc<Shared>>, req: Request) -> Response {
    let headers = req.headers().clone();
    let caller = match shared.caller(super::http::auth_header(&headers)) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let arrived = req
        .extensions()
        .get::<super::grpc::Arrived>()
        .map_or_else(Instant::now, |a| a.0);
    let limit = shared.limits.max_request_bytes;
    let length = match super::http::content_length(&headers) {
        Ok(n) if n > limit => return ApiError::request_too_large(limit).into_response(),
        Ok(n) => n,
        Err(e) => return e.into_response(),
    };
    let service = &shared.router.service;
    // The tools are service calls through the router (#1915 PR 3), so on a cluster they
    // reach every partition; the session keeps one database of its own, the node's first
    // partition's, for whether the change feed is on. A node that only routes has none
    // (D317 ticket 2).
    let db = match shared.router.parts().first() {
        Some(p) => p.db.clone(),
        None => {
            return ApiError::unsupported("/mcp needs a node that serves a database")
                .into_response();
        }
    };
    let budget = match service.reserve(length).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    // Within the request's one deadline (audit120's S-S1).
    let until = arrived + shared.limits.request_timeout;
    let text =
        match tokio::time::timeout_at(until.into(), axum::body::to_bytes(req.into_body(), length))
            .await
        {
            Ok(Ok(b)) => match String::from_utf8(b.to_vec()) {
                Ok(t) => t,
                Err(_) => return ApiError::invalid("the body isn't UTF-8").into_response(),
            },
            Ok(Err(_)) => {
                return ApiError::invalid("the body is longer than its Content-Length")
                    .into_response();
            }
            Err(_) => return ApiError::deadline_exceeded().into_response(),
        };
    // A session this request starts, ended again unless its `initialize` succeeds: a
    // refused one mustn't hold a place against the token's cap for its idle lease
    // (ai1249's nit on #1666).
    let mut started = None;
    let id = match headers.get(SESSION).map(HeaderValue::to_str) {
        Some(Ok(id)) => id.to_owned(),
        Some(Err(_)) => return ApiError::not_found("no such MCP session").into_response(),
        // No session yet: only an `initialize` starts one.
        None if is_initialize(&text) => {
            let session = Arc::new(McpSession::new(&shared, &caller, db.clone()));
            match service.sessions.open_mcp(caller.principal, session) {
                Ok(id) => {
                    started = Some(Started {
                        sessions: Arc::clone(&service.sessions),
                        id: id.clone(),
                        principal: caller.principal,
                        keep: false,
                    });
                    id
                }
                Err(e) => return e.into_response(),
            }
        }
        None => {
            return ApiError::invalid(
                "no Mcp-Session-Id: start a session with an `initialize` request",
            )
            .into_response();
        }
    };
    let guard = match service.sessions.mcp(&id, caller.principal) {
        Ok(g) => g,
        Err(e) => return e.into_response(),
    };
    let session = Arc::clone(guard.session());
    if let (Some(asked), Some(version)) = (
        headers.get(VERSION).map(|v| v.to_str().unwrap_or("")),
        session.protocol.version(),
    ) && asked != version
    {
        return ApiError::invalid(format!(
            "MCP-Protocol-Version {asked:?} isn't this session's ({version})"
        ))
        .into_response();
    }
    let _place = match tokio::time::timeout(
        SESSION_WAIT.min(shared.limits.request_timeout),
        Arc::clone(&session.requests).acquire_owned(),
    )
    .await
    {
        Ok(Ok(p)) => p,
        _ => {
            return ApiError::overloaded(
                "this MCP session is at its request limit (--mcp-session-requests)",
            )
            .into_response();
        }
    };
    // Read-your-writes on a replica (#1577), within the request timeout from its arrival.
    let min_seqno = match super::replica::min_primary_seqno(
        headers
            .get(super::replica::MIN_PRIMARY_SEQNO)
            .map(|v| v.as_bytes()),
    ) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    if let (Some(seqno), Some(replica)) = (min_seqno, &service.replica) {
        let deadline =
            super::replica::ReadDeadline::new(arrived, shared.limits.request_timeout, None);
        if let Err(e) = replica.wait_applied(seqno, deadline).await {
            return e.into_response();
        }
    }
    // Another session's create or drop may have changed the tools (M2).
    let now = service.catalog();
    if session.catalog.swap(now, Ordering::AcqRel) != now {
        session.protocol.catalog_moved();
    }
    // A `feed_poll` that waits waits here, on the runtime, holding no `/mcp` place (the
    // audit's F7, simp724's M2): its subscription opened and its first commit waited for,
    // then handed to `Gate::call`, which takes what's ready.
    let feed = match feed_wait(&text) {
        Some(args) => poll_wait(&shared, &caller, &session, &args, until).await,
        None => None,
    };
    let caller_owned = caller.clone();
    let shared_owned = Arc::clone(&shared);
    // On a blocking thread under a `/mcp` place, outside `--blocking-threads` (ICR 0139's
    // PR 3 section): its tools' ops take their own permits.
    let answered = service
        .mcp_blocking(until.into(), move || {
            let gate = Gate {
                session: &session,
                shared: &shared_owned,
                caller: &caller_owned,
                feed: std::sync::Mutex::new(feed),
            };
            super::mcp_backend::within(until, || {
                let core = Core::new(Transport::Http);
                let reply = core.message(&gate, &session.protocol, &text);
                let note = core.list_changed(&gate, &session.protocol);
                Ok((reply, note))
            })
        })
        .await;
    drop(budget);
    drop(guard);
    let (reply, note) = match answered {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    if let Some(s) = &mut started {
        // Initialized: the session stays.
        s.keep = reply.as_ref().is_some_and(|r| r.get("result").is_some());
    }
    if started.as_ref().is_some_and(|s| !s.keep) {
        // Its `initialize` was refused: the session goes, and its id isn't given out.
        return match reply {
            Some(reply) => body(&reply.to_string(), "application/json"),
            None => StatusCode::ACCEPTED.into_response(),
        };
    }
    // The reply isn't charged to the in-flight budget: a tool result is bounded by
    // `--mcp-max-result-bytes` instead (serve.md, "What isn't counted").
    let mut resp = match (reply, note) {
        (None, None) => StatusCode::ACCEPTED.into_response(),
        (Some(reply), None) => body(&reply.to_string(), "application/json"),
        (reply, Some(note)) => {
            let mut events = String::new();
            for message in reply.iter().chain([&note]) {
                events.push_str("event: message\ndata: ");
                events.push_str(&message.to_string());
                events.push_str("\n\n");
            }
            body(&events, "text/event-stream")
        }
    };
    if let Ok(v) = HeaderValue::from_str(&id) {
        resp.headers_mut().insert(SESSION, v);
    }
    resp
}

/// A session a request started, ended when dropped unless `keep` (its `initialize`
/// succeeded), so every early answer ends it too.
struct Started {
    sessions: Arc<super::sessions::Sessions>,
    id: String,
    principal: crate::serve::auth::Principal,
    keep: bool,
}

impl Drop for Started {
    fn drop(&mut self) {
        if !self.keep {
            let _ = self.sessions.end_mcp(&self.id, self.principal);
        }
    }
}

/// `GET /mcp`: this server opens no stream of its own (its only notification rides on a
/// POST's reply), which Streamable HTTP answers 405.
pub(crate) async fn get() -> Response {
    let mut resp = StatusCode::METHOD_NOT_ALLOWED.into_response();
    resp.headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST, DELETE"));
    resp
}

/// `DELETE /mcp`: ends the session.
pub(crate) async fn delete(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let caller = match shared.caller(super::http::auth_header(&headers)) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let Some(id) = headers.get(SESSION).and_then(|v| v.to_str().ok()) else {
        return ApiError::invalid("DELETE /mcp needs the Mcp-Session-Id to end").into_response();
    };
    match shared.router.service.sessions.end_mcp(id, caller.principal) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => e.into_response(),
    }
}

/// The arguments of a `feed_poll` that waits (`wait_ms` above 0), if `text` is one: a single
/// message, not a batch, whose calls then don't wait (`Gate::call` takes what's ready).
fn feed_wait(text: &str) -> Option<Map<String, Value>> {
    if !text.contains("feed_poll") {
        return None;
    }
    let m = serde_json::from_str::<Value>(text).ok()?;
    if m.get("method").and_then(Value::as_str) != Some("tools/call")
        || m["params"]["name"].as_str() != Some("feed_poll")
    {
        return None;
    }
    let args = m["params"]["arguments"].as_object()?.clone();
    (args.get("wait_ms").and_then(Value::as_u64).unwrap_or(0) > 0).then_some(args)
}

/// Opens the subscription a `feed_poll` with `args` reads, and waits on the runtime for its
/// first commit, up to its `wait_ms`: holding no `/mcp` place or blocking thread while it
/// waits (simp724's M2). Whatever fails, the call then runs and reports it: a caller it
/// refuses, a bad argument or a failed open just skip the wait.
async fn poll_wait(
    shared: &Shared,
    caller: &Caller,
    session: &McpSession,
    args: &Map<String, Value>,
    until: Instant,
) -> Option<ServeFeed> {
    let admit = Admit {
        what: "feed_poll",
        scope: Scope::Read,
        writes: false,
        size: 0,
        memory: false,
        peer: false,
    };
    shared.router.service.check(&admit, caller).ok()?;
    let p = session.tools.feed_poll_args(args).ok()?;
    let mut feed = super::mcp_backend::subscribe(shared, caller, &p.feed(), Some(until))
        .await
        .ok()?;
    let wait = p.wait.min(until.saturating_duration_since(Instant::now()));
    feed.wait(wait).await.ok()?;
    Some(feed)
}

/// Whether `text` is one `initialize` request (not a batch: 2025-03-26 forbids one there).
fn is_initialize(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .is_ok_and(|v| v.get("method").and_then(Value::as_str) == Some("initialize"))
}

fn body(text: &str, content_type: &'static str) -> Response {
    let mut resp = (StatusCode::OK, Body::from(text.to_owned())).into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    resp
}
