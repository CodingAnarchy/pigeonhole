//! The MCP protocol for one session, apart from its transport (D313 ticket 8c): what the
//! server answers to `initialize`, `ping`, `tools/list`, `tools/call` and `resources/*`, and
//! when it owes the client `notifications/tools/list_changed`. `phdb mcp`'s stdio loop drives
//! it one line at a time; `phdb serve`'s `/mcp` drives it one POST at a time, a session per
//! `Mcp-Session-Id`.
//!
//! A tool's own failure (bad arguments, a database error) is a result with `isError`, which
//! the agent sees and can act on; only protocol mistakes (an unknown method or tool,
//! malformed JSON-RPC) are JSON-RPC errors.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use serde_json::{Map, Value, json};

use super::memory::{GUIDE, Memory};
use super::tools::Outcome;

/// The memory guide's resource.
const GUIDE_URI: &str = "memory://guide";

/// What the agent is told at `initialize`.
const INSTRUCTIONS: &str = "\
A Pigeonhole database: a wide-column store. A table holds rows sorted by key; a row holds \
cells in column families (fixed per table, listed by list_tables); a cell is addressed by \
family and qualifier and keeps timestamped versions (microseconds). Row keys, qualifiers and \
byte values are JSON strings when UTF-8, else {\"base64\": \"...\"}; integer and float cells \
are {\"i64\": n} and {\"f64\": x}. Start with list_tables. A result cut short to keep it small \
carries a `cursor`: repeat the same call with that cursor to get the rest.";

/// JSON-RPC error codes.
pub(super) const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
/// MCP's code for a resource that doesn't exist.
const RESOURCE_NOT_FOUND: i64 = -32002;

/// How the client reaches the server: each transport speaks its own protocol versions
/// (ai1249's M3 on D313). The tool shapes are the same in each; `structuredContent`
/// (2025-06-18) is sent to every client, and older ones ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    /// `phdb mcp`: one JSON-RPC message per line on standard input and output.
    Stdio,
    /// `phdb serve`'s `/mcp`: Streamable HTTP. 2024-11-05 defined the older HTTP+SSE
    /// transport instead, so it isn't offered there.
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    Http,
}

impl Transport {
    /// The protocol versions it speaks, newest first.
    pub(crate) fn versions(self) -> &'static [&'static str] {
        const ALL: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
        match self {
            Self::Stdio => ALL,
            Self::Http => &ALL[..3],
        }
    }
}

/// What a session's requests reach: the tools and the memory store.
pub(crate) trait Host {
    /// The memory store, with `--memory`.
    fn memory(&self) -> Option<&Memory>;
    /// The tools, as `tools/list` gives them.
    fn list(&self) -> Vec<Value>;
    /// The tools' names, to tell whether the list changed.
    fn names(&self) -> Vec<String>;
    /// A tool's call, or `None` for a name that isn't one.
    fn call(&self, name: &str, args: &Map<String, Value>) -> Option<Outcome>;
}

/// One client's session. Its state sits behind its own small locks, so a server can answer
/// several of a session's requests at once (ai1249's N1 on #1660): a message never holds
/// one across a tool call.
#[derive(Debug, Default)]
pub(crate) struct Session {
    /// The tool names the client was last given (`tools/list`): when a call changes them
    /// (`create_table` adding the first vector family brings `nearest`), the server sends
    /// `notifications/tools/list_changed` (#1356). `None` until the client lists.
    listed: Mutex<Option<Vec<String>>>,
    /// Whether a message since the last check held a `tools/call`, or the catalog may have
    /// changed under the session: only then is the list checked (ai1249 on #1379), so a
    /// ping or a notification costs no catalog walk.
    called: AtomicBool,
    /// The protocol version `initialize` settled on, once it has: on `/mcp`, a later
    /// request's `MCP-Protocol-Version` must match it.
    version: OnceLock<&'static str>,
}

impl Session {
    /// The protocol version `initialize` settled on, once it has.
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    pub(crate) fn version(&self) -> Option<&'static str> {
        self.version.get().copied()
    }

    /// The catalog may have changed under the session (another session's `create_table`,
    /// on a shared server): check the list after the next message, as after a call (M2).
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    pub(crate) fn catalog_moved(&self) {
        self.called.store(true, Ordering::Relaxed);
    }

    fn listed(&self) -> MutexGuard<'_, Option<Vec<String>>> {
        // A panic while it was held left a whole value: every change is one store.
        self.listed.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The protocol over one transport.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Core {
    transport: Transport,
}

impl Core {
    pub(crate) fn new(transport: Transport) -> Self {
        Self { transport }
    }
    /// One message's text: a request, a notification or a batch (2025-03-26). Returns the
    /// reply, if any is due.
    pub(crate) fn message(&self, host: &dyn Host, s: &Session, text: &str) -> Option<Value> {
        let msg: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => return Some(error(Value::Null, PARSE_ERROR, &format!("not JSON: {e}"))),
        };
        match msg {
            Value::Array(batch) if !batch.is_empty() => {
                let replies: Vec<Value> = batch
                    .into_iter()
                    .filter_map(|m| self.one(host, s, m))
                    .collect();
                (!replies.is_empty()).then_some(Value::Array(replies))
            }
            other => self.one(host, s, other),
        }
    }

    /// `notifications/tools/list_changed` if a call since the last check changed the tools
    /// from those last listed, once per change.
    pub(crate) fn list_changed(&self, host: &dyn Host, s: &Session) -> Option<Value> {
        if !s.called.swap(false, Ordering::Relaxed) {
            return None;
        }
        if s.listed().is_none() {
            return None;
        }
        // The catalog walk runs outside the lock; a concurrent `tools/list` may have
        // replaced the list meanwhile, and the comparison is with whatever is there now.
        let now = host.names();
        let mut listed = s.listed();
        if listed.as_ref().is_none_or(|before| *before == now) {
            return None;
        }
        *listed = Some(now);
        Some(json!({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}))
    }

    fn one(&self, host: &dyn Host, s: &Session, msg: Value) -> Option<Value> {
        let Value::Object(msg) = msg else {
            return Some(error(
                Value::Null,
                INVALID_REQUEST,
                "not a JSON-RPC message",
            ));
        };
        let id = msg.get("id").cloned();
        let Some(method) = msg.get("method").and_then(Value::as_str) else {
            // A response to a request of ours (the server sends none), or garbage.
            return id.map(|id| error(id, INVALID_REQUEST, "not a request"));
        };
        // A notification (`notifications/initialized`, `notifications/cancelled`) needs no
        // reply, and this server acts on none.
        let id = id?;
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        Some(match self.request(host, s, method, &params) {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, message)) => error(id, code, &message),
        })
    }

    fn request(
        &self,
        host: &dyn Host,
        s: &Session,
        method: &str,
        params: &Value,
    ) -> Result<Value, (i64, String)> {
        match method {
            "initialize" => {
                let asked = params["protocolVersion"].as_str().unwrap_or("");
                let versions = self.transport.versions();
                let version = versions
                    .iter()
                    .find(|&&v| v == asked)
                    .copied()
                    .unwrap_or(versions[0]);
                // A repeated `initialize` keeps the first one's version.
                let version = *s.version.get_or_init(|| version);
                let mut capabilities = json!({"tools": {"listChanged": true}});
                let mut instructions = INSTRUCTIONS.to_owned();
                if let Some(m) = host.memory() {
                    capabilities["resources"] = json!({});
                    instructions.push_str("\n\n");
                    instructions.push_str(&m.instructions());
                }
                Ok(json!({
                    "protocolVersion": version,
                    "capabilities": capabilities,
                    "serverInfo": {
                        "name": "phdb",
                        "title": "Pigeonhole",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "instructions": instructions,
                }))
            }
            "ping" => Ok(json!({})),
            "tools/list" => {
                let tools = host.list();
                *s.listed() = Some(
                    tools
                        .iter()
                        .filter_map(|t| t["name"].as_str().map(str::to_owned))
                        .collect(),
                );
                Ok(json!({"tools": tools}))
            }
            "tools/call" => {
                s.called.store(true, Ordering::Relaxed);
                let name = params["name"]
                    .as_str()
                    .ok_or((INVALID_PARAMS, "tools/call needs a tool name".to_owned()))?;
                let empty = Map::new();
                let args = match &params["arguments"] {
                    Value::Null => &empty,
                    Value::Object(m) => m,
                    _ => return Err((INVALID_PARAMS, "arguments must be an object".into())),
                };
                let outcome = host
                    .call(name, args)
                    .ok_or_else(|| (INVALID_PARAMS, format!("no tool {name}")))?;
                Ok(tool_result(outcome))
            }
            "resources/list" if host.memory().is_some() => Ok(json!({
                "resources": [{
                    "uri": GUIDE_URI,
                    "name": "memory-guide",
                    "title": "Using the memory tools",
                    "description": "When to remember and recall, namespaces, importance, \
                                    superseding, marking use, and embeddings.",
                    "mimeType": "text/markdown",
                }],
            })),
            "resources/read" if host.memory().is_some() => {
                let uri = params["uri"]
                    .as_str()
                    .ok_or((INVALID_PARAMS, "resources/read needs a uri".to_owned()))?;
                if uri != GUIDE_URI {
                    return Err((RESOURCE_NOT_FOUND, format!("no resource {uri}")));
                }
                Ok(json!({
                    "contents": [{"uri": GUIDE_URI, "mimeType": "text/markdown", "text": GUIDE}],
                }))
            }
            _ => Err((METHOD_NOT_FOUND, format!("no method {method}"))),
        }
    }
}

/// A `tools/call` result: the JSON as text (for every client) and as `structuredContent`.
fn tool_result(outcome: Outcome) -> Value {
    let (body, is_error) = match outcome {
        Ok(m) => (Value::Object(m), false),
        Err(f) => (json!({"error": f.json()}), true),
    };
    json!({
        "content": [{"type": "text", "text": body.to_string()}],
        "structuredContent": body,
        "isError": is_error,
    })
}

pub(super) fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[cfg(test)]
mod tests {
    use super::Transport;

    #[test]
    fn each_transport_speaks_its_own_protocol_versions() {
        // ai1249's M3 on D313: 2024-11-05's HTTP transport is HTTP+SSE, so Streamable HTTP
        // offers 2025-03-26 and later only.
        assert_eq!(Transport::Stdio.versions().last(), Some(&"2024-11-05"));
        assert!(!Transport::Http.versions().contains(&"2024-11-05"));
        assert_eq!(Transport::Http.versions().last(), Some(&"2025-03-26"));
        assert_eq!(
            Transport::Http.versions()[0],
            Transport::Stdio.versions()[0]
        );
    }
}
