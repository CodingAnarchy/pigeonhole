//! `phdb mcp`: a Model Context Protocol server on standard input and output (#1249).
//!
//! The stdio transport: one JSON-RPC 2.0 message per line in each direction, and nothing
//! else on standard output (diagnostics go to standard error). The server answers
//! `initialize`, `ping`, `tools/list` and `tools/call`, ignores notifications, and exits
//! when its input ends, closing the database.
//!
//! A hand-written loop rather than an MCP SDK: the server is synchronous and needs four
//! methods, and the Rust SDKs bring an async runtime and a schema generator for them.
//!
//! The tools are in [`tools`]. A tool's own failure (bad arguments, a database error) is a
//! result with `isError`, which the agent sees and can act on; only protocol mistakes (an
//! unknown method or tool, malformed JSON-RPC) are JSON-RPC errors.

use std::io::{BufRead, Write};
use std::path::Path;

use serde_json::{Map, Value, json};

use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::open::{self, Db};

mod cursor;
mod tools;

/// The protocol versions this server speaks, newest first. The tool shapes are the same in
/// each; `structuredContent` (2025-06-18) is sent to every client, and older ones ignore it.
const PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// What the agent is told at `initialize`.
const INSTRUCTIONS: &str = "\
A Pigeonhole database: a wide-column store. A table holds rows sorted by key; a row holds \
cells in column families (fixed per table, listed by list_tables); a cell is addressed by \
family and qualifier and keeps timestamped versions (microseconds). Row keys, qualifiers and \
byte values are JSON strings when UTF-8, else {\"base64\": \"...\"}; integer and float cells \
are {\"i64\": n} and {\"f64\": x}. Start with list_tables. A result cut short to keep it small \
carries a `cursor`: repeat the same call with that cursor to get the rest.";

/// Result-size limits (`--max-result-bytes`, `--max-value-bytes`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// A result's size, about, in bytes of its JSON.
    pub result_bytes: usize,
    /// The longest value a read returns whole unless it asks for more.
    pub value_bytes: usize,
}

/// JSON-RPC error codes.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    read_only: bool,
    create: bool,
    limits: Limits,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<ExitStatus, Failure> {
    let db = if read_only {
        open::reader_only(cli, file)?
    } else if create && !file.exists() {
        Db::Writer(open::for_write(cli, file, true)?)
    } else {
        open::for_read(cli, file)?
    };
    let writable = !read_only && db.writable();
    let _ = writeln!(
        stderr,
        "phdb mcp: serving {} as {}{}",
        file.display(),
        db.mode(),
        if writable { "" } else { " (read-only)" }
    );
    let server = Server {
        tools: tools::Tools::new(db, writable, limits),
    };
    let served = server.serve(&mut std::io::stdin().lock(), stdout);
    let closed = server.tools.close();
    if let Err(e) = served {
        // A client that went away mid-reply is the end of the session, not a failure.
        if e.kind() != std::io::ErrorKind::BrokenPipe {
            return Err(Failure::io("mcp", &e));
        }
    }
    closed?;
    Ok(ExitStatus::SUCCESS)
}

struct Server {
    tools: tools::Tools,
}

impl Server {
    /// Answers each line of `input` on `output` until the input ends.
    fn serve(&self, input: &mut dyn BufRead, output: &mut dyn Write) -> std::io::Result<()> {
        let mut line = String::new();
        loop {
            line.clear();
            if input.read_line(&mut line)? == 0 {
                return Ok(());
            }
            if line.trim().is_empty() {
                continue;
            }
            if let Some(reply) = self.message(&line) {
                writeln!(output, "{reply}")?;
                output.flush()?;
            }
        }
    }

    /// One line: a request, a notification or a batch (2025-03-26). Returns the reply, if
    /// any is due.
    fn message(&self, line: &str) -> Option<Value> {
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => return Some(error(Value::Null, PARSE_ERROR, &format!("not JSON: {e}"))),
        };
        match msg {
            Value::Array(batch) if !batch.is_empty() => {
                let replies: Vec<Value> = batch.into_iter().filter_map(|m| self.one(m)).collect();
                (!replies.is_empty()).then_some(Value::Array(replies))
            }
            other => self.one(other),
        }
    }

    fn one(&self, msg: Value) -> Option<Value> {
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
        Some(match self.request(method, &params) {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err((code, message)) => error(id, code, &message),
        })
    }

    fn request(&self, method: &str, params: &Value) -> Result<Value, (i64, String)> {
        match method {
            "initialize" => {
                let asked = params["protocolVersion"].as_str().unwrap_or("");
                let version = PROTOCOL_VERSIONS
                    .iter()
                    .find(|&&v| v == asked)
                    .unwrap_or(&PROTOCOL_VERSIONS[0]);
                Ok(json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {
                        "name": "phdb",
                        "title": "Pigeonhole",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "instructions": INSTRUCTIONS,
                }))
            }
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({"tools": self.tools.list()})),
            "tools/call" => {
                let name = params["name"]
                    .as_str()
                    .ok_or((INVALID_PARAMS, "tools/call needs a tool name".to_owned()))?;
                let empty = Map::new();
                let args = match &params["arguments"] {
                    Value::Null => &empty,
                    Value::Object(m) => m,
                    _ => return Err((INVALID_PARAMS, "arguments must be an object".into())),
                };
                let outcome = self
                    .tools
                    .call(name, args)
                    .ok_or_else(|| (INVALID_PARAMS, format!("no tool {name}")))?;
                Ok(tool_result(outcome))
            }
            _ => Err((METHOD_NOT_FOUND, format!("no method {method}"))),
        }
    }
}

/// A `tools/call` result: the JSON as text (for every client) and as `structuredContent`.
fn tool_result(outcome: Result<Map<String, Value>, Failure>) -> Value {
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

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}
