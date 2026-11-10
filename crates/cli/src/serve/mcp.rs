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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pigeonhole::{Family, FamilyInfo, Pigeonhole, Table};
use serde_json::{Map, Value};
use tokio::sync::Semaphore;

use super::Shared;
use super::auth::{Caller, Scope};
use super::errors::ApiError;
use super::service::Admit;
use crate::exit::Failure;
use crate::mcp::backend::Backend;
use crate::mcp::memory::{Memory, Store as MemStore};
use crate::mcp::protocol::{self, Core, Host, Transport};
use crate::mcp::tools::{Outcome, Tools};
use crate::memcore::MemCore;
use crate::open::Handle;

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
            db,
            writable,
            replica: service.replica.is_some(),
        };
        // The memory tools over the served store (8c-3b), limited to the caller's namespaces
        // (M4), as `Memory`'s RPCs are.
        let memory = service.memory_slot().map(|slot| {
            Memory::served(
                slot.name(),
                Box::new(ServeStore {
                    shared: Arc::downgrade(shared),
                }),
                caller.namespaces.clone(),
                writable,
                slot.embeds(),
                shared.limits.mcp.result_bytes,
                slot.max_recall(),
            )
        });
        Self {
            tools: Tools::new(Box::new(backend), writable, shared.limits.mcp, memory),
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

/// The tools' database surface on the service layer (D313 §7 as amended in 8c-2). Its
/// calls run on the blocking thread a `/mcp` request already holds, so they use the
/// service's sync paths (`table_blocking`, `create_blocking`): taking a second pool thread
/// from inside one could wait forever on a full pool (ai1249's B1 on #1666).
struct ServeBackend {
    /// Weak: the session table, inside the service, holds the session that holds this.
    shared: Weak<Shared>,
    /// The served database, for a `write` batch. A replica's re-bootstrap replaces the
    /// engine under this handle, not the handle.
    db: Pigeonhole,
    writable: bool,
    replica: bool,
}

impl ServeBackend {
    fn shared(&self) -> Result<Arc<Shared>, Failure> {
        self.shared
            .upgrade()
            .ok_or_else(|| failure(ApiError::unavailable("the server is shutting down")))
    }

    /// `table`, through the service's own table cache and its rules.
    fn table(&self, table: &str) -> Result<Table, Failure> {
        let shared = self.shared()?;
        shared
            .router
            .service
            .table_blocking(table)
            .map_err(|e| failure(e).within(&format!("table {table}")))
    }
}

impl Backend for ServeBackend {
    fn mode(&self) -> &'static str {
        if self.replica { "replica" } else { "server" }
    }

    fn lists_feed(&self) -> bool {
        self.db.info().is_ok_and(|i| i.feed.is_some())
    }

    fn tables(&self) -> Vec<String> {
        self.shared()
            .map(|s| s.router.service.user_tables())
            .unwrap_or_default()
    }

    fn families(&self, table: &str) -> Result<Vec<FamilyInfo>, Failure> {
        let t = self.table(table)?;
        Ok(t.families()
            .into_iter()
            .filter_map(|f| t.family_info(f))
            .collect())
    }

    fn handle(&self, table: &str) -> Result<Handle, Failure> {
        self.table(table).map(Handle::Writer)
    }

    fn writer(&self) -> Option<&Pigeonhole> {
        // Safe to hand out: the batch commits only to tables `open_for_write` gave (the
        // service's own handles), and `write` passed `admit` with the write scope.
        self.writable.then_some(&self.db)
    }

    fn open_for_write(&self, table: &str) -> Option<Result<Table, Failure>> {
        self.writable.then(|| self.table(table))
    }

    fn create_table(
        &self,
        table: &str,
        families: Vec<(String, Family)>,
    ) -> Option<Result<Table, Failure>> {
        if !self.writable {
            return None;
        }
        Some(self.shared().and_then(|shared| {
            shared
                .router
                .service
                .create_blocking(table, families, true)
                .map_err(|e| failure(e).within(&format!("create_table {table}")))
        }))
    }

    fn close(self: Box<Self>) -> Result<(), Failure> {
        Ok(())
    }
}

/// The served memory store, as the memory tools reach it: through the service's own slot,
/// which a catalog change or a replica's re-bootstrap opens again.
struct ServeStore {
    /// Weak, as [`ServeBackend`]'s.
    shared: Weak<Shared>,
}

impl MemStore for ServeStore {
    fn core(&self) -> Result<Arc<MemCore>, Failure> {
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| failure(ApiError::unavailable("the server is shutting down")))?;
        // On the request's pool thread, under the memory place `post` took first.
        shared
            .router
            .service
            .memory_core_blocking()
            .map_err(failure)
    }

    fn cached(&self) -> Option<Arc<MemCore>> {
        let shared = self.shared.upgrade()?;
        match shared.router.service.memory_slot()?.cached()? {
            super::memslot::Entry::Core(core) => Some(core),
            _ => None,
        }
    }
}

/// A service refusal as a tool's failure: its code and message.
fn failure(e: ApiError) -> Failure {
    Failure::served(&e.code, e.code_num, &e.message)
}

/// What one `/mcp` request's messages reach: the session's tools, each call admitted first.
struct Gate<'a> {
    session: &'a McpSession,
    shared: &'a Shared,
    caller: &'a Caller,
    /// Whether `post` took a memory place (M6) for this message, before its pool thread.
    memory_place: bool,
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
        };
        // The checks only: the request's bytes are its POST's, already reserved (ai1249's S1).
        if let Err(e) = service.check(&admit, self.caller) {
            return Some(Err(failure(e)));
        }
        // A memory tool runs under a memory place (M6). `post` takes it before the pool
        // thread for a message it sees calling one. One it didn't see (a tool name spelled
        // with JSON escapes) fails closed: a place now, without waiting (a wait here, holding
        // the pool thread, is the order the RPCs never take), else `Overloaded` (ai1249's R1
        // on #1679).
        let _memory = if name.starts_with("memory_") && !self.memory_place {
            match service.memory_try_place() {
                Ok(p) => Some(p),
                Err(e) => return Some(Err(failure(e))),
            }
        } else {
            None
        };
        // A poll can wait up to 30 s: it holds a `--max-streams` place for that long, as a
        // feed stream does (ai1249's note on 8c), though it needs no hub.
        let _stream = if name == "feed_poll" {
            match service.stream_slot() {
                Ok(p) => Some(p),
                Err(e) => return Some(Err(failure(e))),
            }
        } else {
            None
        };
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
    // A cluster node's tools would see only its own partition: refused until a D317 ticket
    // routes them (cli692's 4 on ticket 2).
    if shared.router.cluster_node().is_some() {
        return ApiError::unsupported("/mcp on a cluster node comes with a later D317 ticket")
            .into_response();
    }
    let service = &shared.router.service;
    // `/mcp` serves one database's tools: a node that only routes has none (D317 ticket 2).
    let db = match service.part() {
        Ok(p) => p.db.clone(),
        Err(_) => {
            return ApiError::unsupported("/mcp needs a node that serves a database")
                .into_response();
        }
    };
    let budget = match service.reserve(length).await {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let text = match axum::body::to_bytes(req.into_body(), length).await {
        Ok(b) => match String::from_utf8(b.to_vec()) {
            Ok(t) => t,
            Err(_) => return ApiError::invalid("the body isn't UTF-8").into_response(),
        },
        Err(_) => {
            return ApiError::invalid("the body is longer than its Content-Length").into_response();
        }
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
            match service.sessions.open_mcp(caller.token, session) {
                Ok(id) => {
                    started = Some(Started {
                        sessions: Arc::clone(&service.sessions),
                        id: id.clone(),
                        token: caller.token,
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
    let guard = match service.sessions.mcp(&id, caller.token) {
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
    // A memory tool runs under the memory sub-limit (M6), whose place is taken before the
    // pool's, as the RPCs take it: never the other way round (see `Service::memory_place`).
    let _memory = if calls_memory(&text) {
        match service.memory_place().await {
            Ok(p) => Some(p),
            Err(e) => return e.into_response(),
        }
    } else {
        None
    };
    let memory_place = _memory.is_some();
    let caller_owned = caller.clone();
    let shared_owned = Arc::clone(&shared);
    let answered = service
        .run_blocking(move || {
            let gate = Gate {
                session: &session,
                shared: &shared_owned,
                caller: &caller_owned,
                memory_place,
            };
            let core = Core::new(Transport::Http);
            let reply = core.message(&gate, &session.protocol, &text);
            let note = core.list_changed(&gate, &session.protocol);
            Ok((reply, note))
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
    // TODO(#1567): charge the reply to the in-flight budget until it's written, through
    // #1661's charge layer once it lands (before 1.2.0 ships; noted on #1567).
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
    token: u32,
    keep: bool,
}

impl Drop for Started {
    fn drop(&mut self) {
        if !self.keep {
            let _ = self.sessions.end_mcp(&self.id, self.token);
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
    match shared.router.service.sessions.end_mcp(id, caller.token) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => e.into_response(),
    }
}

/// Whether `text` (a message or a batch) calls a memory tool.
fn calls_memory(text: &str) -> bool {
    // Most messages name no memory tool: skip the parse (ai1249's N3 on #1679). One whose
    // name is spelled with JSON escapes gets past this; `Gate::call` catches it (R1).
    if !text.contains("\"memory_") {
        return false;
    }
    let call = |m: &Value| {
        m.get("method").and_then(Value::as_str) == Some("tools/call")
            && m["params"]["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("memory_"))
    };
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(batch)) => batch.iter().any(call),
        Ok(m) => call(&m),
        Err(_) => false,
    }
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
