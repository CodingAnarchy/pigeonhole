//! `phdb mcp` (#1249), driven as an MCP client drives it: the `phdb` binary as a child
//! process, one JSON-RPC message per line on its standard input and output.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use common::TempDir;
use pigeonhole::{Options, Pigeonhole};
use serde_json::{Value, json};

/// Whether this thread's servers are `phdb serve`'s `/mcp` rather than `phdb mcp` (D313
/// ticket 8c-3a): the [`over_http`] tests run the same bodies with it set, so both
/// transports must give the same answers.
mod transport {
    use std::cell::Cell;

    thread_local! {
        static HTTP: Cell<bool> = const { Cell::new(false) };
    }

    pub fn http() -> bool {
        HTTP.with(Cell::get)
    }

    #[cfg_attr(not(all(feature = "serve", unix)), allow(dead_code))]
    pub fn set_http() {
        HTTP.with(|h| h.set(true));
    }
}

/// A running `phdb mcp`, or `phdb serve` driven over `/mcp`.
struct Server {
    child: Child,
    io: Io,
    next_id: u64,
    /// Notifications the server sent, in order (#1356).
    notes: Vec<Value>,
}

enum Io {
    /// One JSON-RPC message per line on the child's standard input and output.
    Stdio {
        stdin: Option<ChildStdin>,
        stdout: BufReader<ChildStdout>,
    },
    /// `POST /mcp`, one message per request; the replies wait in `replies`.
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    Http {
        /// `None` when the server exited before it served (a startup refusal).
        url: Option<String>,
        agent: ureq::Agent,
        session: Option<String>,
        version: Option<String>,
        replies: std::collections::VecDeque<Value>,
        stderr: std::sync::mpsc::Receiver<String>,
    },
}

impl Server {
    /// Starts `phdb mcp <path> <flags>` and completes the handshake.
    fn start(path: &Path, flags: &[&str]) -> Self {
        Self::start_env(path, flags, &[]).0
    }

    /// [`start`](Self::start) with environment variables, returning the `initialize` result
    /// too.
    fn start_env(path: &Path, flags: &[&str], env: &[(&str, &str)]) -> (Self, Value) {
        let mut s = Self::spawn_env(path, flags, env);
        let init = s.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            }),
        );
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18", "{init}");
        s.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        (s, init["result"].clone())
    }

    fn spawn(path: &Path, flags: &[&str]) -> Self {
        Self::spawn_env(path, flags, &[])
    }

    /// [`spawn`](Self::spawn) with environment variables set for the server.
    fn spawn_env(path: &Path, flags: &[&str], env: &[(&str, &str)]) -> Self {
        if transport::http() {
            return Self::spawn_http(path, flags, env);
        }
        let mut child = Command::new(env!("CARGO_BIN_EXE_phdb"))
            .arg("mcp")
            .arg(path)
            .args(flags)
            .envs(env.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            io: Io::Stdio { stdin, stdout },
            next_id: 1,
            notes: Vec::new(),
        }
    }

    /// `phdb serve <path>` on a loopback port, with `phdb mcp`'s flags given their `/mcp`
    /// names.
    fn spawn_http(path: &Path, flags: &[&str], env: &[(&str, &str)]) -> Self {
        let flags: Vec<&str> = flags
            .iter()
            .map(|f| match *f {
                "--max-result-bytes" => "--mcp-max-result-bytes",
                "--max-value-bytes" => "--mcp-max-value-bytes",
                "--read-only" => panic!("phdb serve has no --read-only: test a read token"),
                other => other,
            })
            .collect();
        let mut child = Command::new(env!("CARGO_BIN_EXE_phdb"))
            .arg("serve")
            .arg(path)
            .args(["--listen", "127.0.0.1:0"])
            .args(&flags)
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // Its stderr, line by line: the first `serving ... on <addr>` gives the port.
        let (tx, rx) = std::sync::mpsc::channel();
        let stderr = BufReader::new(child.stderr.take().unwrap());
        std::thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        // Lines before it are kept for `finish`; a server that exits first (a startup
        // refusal) closes its stderr, and `finish` reports its status.
        let (keep, rx_kept) = std::sync::mpsc::channel();
        let mut url = None;
        while url.is_none() {
            let line = match rx.recv_timeout(std::time::Duration::from_secs(60)) {
                Ok(line) => line,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(e) => panic!("phdb serve didn't start: {e}"),
            };
            if let Some(rest) = line.split(" on ").nth(1)
                && line.starts_with("phdb serve: serving")
            {
                let addr = rest.split_whitespace().next().unwrap();
                url = Some(format!("http://{addr}/mcp"));
            }
            keep.send(line).unwrap();
        }
        // The rest of its stderr, after the lines already read.
        std::thread::spawn(move || {
            for line in rx {
                if keep.send(line).is_err() {
                    return;
                }
            }
        });
        let rx = rx_kept;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            child,
            io: Io::Http {
                url,
                agent,
                session: None,
                version: None,
                replies: std::collections::VecDeque::new(),
                stderr: rx,
            },
            next_id: 1,
            notes: Vec::new(),
        }
    }

    fn send(&mut self, msg: &Value) {
        self.send_line(&msg.to_string());
    }

    fn send_line(&mut self, line: &str) {
        match &mut self.io {
            Io::Stdio { stdin, .. } => {
                let stdin = stdin.as_mut().unwrap();
                writeln!(stdin, "{line}").unwrap();
                stdin.flush().unwrap();
            }
            Io::Http {
                url,
                agent,
                session,
                version,
                replies,
                ..
            } => {
                let url = url.as_deref().expect("phdb serve exited at startup");
                let mut req = agent
                    .post(url)
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream");
                if let Some(id) = session {
                    req = req.header("mcp-session-id", id.as_str());
                }
                if let Some(v) = version {
                    req = req.header("mcp-protocol-version", v.as_str());
                }
                let mut resp = req.send(line).unwrap();
                let status = resp.status().as_u16();
                if let Some(id) = resp.headers().get("mcp-session-id") {
                    *session = Some(id.to_str().unwrap().to_owned());
                }
                let sse = resp
                    .headers()
                    .get("content-type")
                    .is_some_and(|v| v.to_str().unwrap().starts_with("text/event-stream"));
                let body = resp.body_mut().read_to_string().unwrap();
                let messages: Vec<Value> = match status {
                    202 => {
                        assert!(body.is_empty(), "{body}");
                        Vec::new()
                    }
                    200 if sse => body
                        .lines()
                        .filter_map(|l| l.strip_prefix("data: "))
                        .map(|d| serde_json::from_str(d).unwrap())
                        .collect(),
                    200 => vec![serde_json::from_str(&body).unwrap()],
                    _ => panic!("POST /mcp: {status} {body}"),
                };
                for m in messages {
                    if m.get("id").is_none() && m.get("method").is_some() {
                        self.notes.push(m);
                        continue;
                    }
                    if let Some(v) = m["result"]["protocolVersion"].as_str() {
                        *version = Some(v.to_owned());
                    }
                    replies.push_back(m);
                }
            }
        }
    }

    /// The next reply; notifications before it are kept in `notes`.
    fn reply(&mut self) -> Value {
        let stdout = match &mut self.io {
            Io::Stdio { stdout, .. } => stdout,
            Io::Http { replies, .. } => return replies.pop_front().expect("no reply"),
        };
        loop {
            let mut line = String::new();
            assert!(
                stdout.read_line(&mut line).unwrap() > 0,
                "the server exited"
            );
            let v: Value = serde_json::from_str(&line).unwrap();
            if v.get("id").is_none() && v.get("method").is_some() {
                self.notes.push(v);
                continue;
            }
            return v;
        }
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let reply = self.reply();
        assert_eq!(reply["id"], id, "{reply}");
        reply
    }

    /// Calls `tool`, returning its result's structured content and whether it is an error.
    fn call_raw(&mut self, tool: &str, args: Value) -> (Value, bool) {
        let reply = self.request("tools/call", json!({"name": tool, "arguments": args}));
        let result = &reply["result"];
        // The text content is the same JSON, for clients without structured content.
        let text: Value = serde_json::from_str(result["content"][0]["text"].as_str().unwrap())
            .unwrap_or_else(|e| panic!("{reply}: {e}"));
        assert_eq!(text, result["structuredContent"]);
        (text, result["isError"].as_bool().unwrap())
    }

    /// Calls `tool` and expects it to succeed.
    fn call(&mut self, tool: &str, args: Value) -> Value {
        let (v, err) = self.call_raw(tool, args.clone());
        assert!(!err, "{tool} {args}: {v}");
        v
    }

    /// Calls `tool` and expects it to fail, returning the error.
    fn fail(&mut self, tool: &str, args: Value) -> Value {
        let (v, err) = self.call_raw(tool, args.clone());
        assert!(err, "{tool} {args}: {v}");
        v["error"].clone()
    }

    fn tool_names(&mut self) -> Vec<String> {
        let list = self.request("tools/list", json!({}));
        list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect()
    }

    /// Ends the input (over HTTP: the session, then the server, with SIGTERM) and waits for
    /// the server to exit, returning its status and stderr.
    fn finish(mut self) -> (i32, String) {
        match &mut self.io {
            Io::Stdio { stdin, .. } => drop(stdin.take()),
            Io::Http {
                url,
                agent,
                session,
                ..
            } => {
                if let (Some(url), Some(id)) = (url.as_deref(), session.as_deref()) {
                    let resp = agent
                        .delete(url)
                        .header("mcp-session-id", id)
                        .call()
                        .unwrap();
                    assert_eq!(resp.status().as_u16(), 204);
                }
                // A server that refused to start has exited already.
                if url.is_some() {
                    let killed = Command::new("kill")
                        .args(["-TERM", &self.child.id().to_string()])
                        .status()
                        .unwrap();
                    assert!(killed.success());
                }
            }
        }
        let out = self.child.wait_with_output().unwrap();
        let stderr = match &self.io {
            Io::Stdio { .. } => String::from_utf8_lossy(&out.stderr).into_owned(),
            Io::Http { stderr, .. } => stderr.iter().collect::<Vec<_>>().join("\n"),
        };
        (out.status.code().unwrap_or(-1), stderr)
    }
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("m.phdb")
}

/// Every page of `tool` from `args` on, following cursors; the items `key` names, joined.
fn pages(s: &mut Server, tool: &str, args: Value, key: &str) -> (Vec<Value>, usize) {
    let mut all = Vec::new();
    let mut args = args;
    let mut calls = 0;
    loop {
        let out = s.call(tool, args.clone());
        calls += 1;
        all.extend(out[key].as_array().unwrap().iter().cloned());
        match out.get("cursor") {
            Some(c) => args["cursor"] = c.clone(),
            None => return (all, calls),
        }
        assert!(calls < 1_000, "cursors never end");
    }
}

#[test]
fn a_client_handshakes_and_lists_the_tools() {
    let dir = TempDir::new("mcp-handshake");
    let path = db_path(&dir);
    let mut s = Server::spawn(&path, &["--create"]);
    // A version the server doesn't know gets its newest.
    let init = s.request("initialize", json!({"protocolVersion": "1999-01-01"}));
    assert_eq!(init["result"]["protocolVersion"], "2025-11-25");
    assert_eq!(init["result"]["serverInfo"]["name"], "phdb");
    assert!(init["result"]["capabilities"]["tools"].is_object());
    // A notification gets no reply: the next line answers the ping.
    s.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    let ping = s.request("ping", json!({}));
    assert_eq!(ping["result"], json!({}));
    let list = s.request("tools/list", json!({}));
    let tools = list["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        [
            "list_tables",
            "read_row",
            "scan",
            "traverse",
            "write",
            "create_table"
        ]
    );
    for t in tools {
        assert_eq!(t["inputSchema"]["type"], "object", "{t}");
        assert!(t["description"].as_str().unwrap().len() > 80, "{t}");
        let read_only = t["annotations"]["readOnlyHint"].as_bool().unwrap();
        assert_eq!(
            read_only,
            !["write", "create_table"].contains(&t["name"].as_str().unwrap())
        );
    }
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    if !transport::http() {
        assert!(err.contains("as writer"), "{err}");
    }
}

#[test]
fn protocol_mistakes_are_json_rpc_errors_and_tool_mistakes_are_results() {
    let dir = TempDir::new("mcp-errors");
    let mut s = Server::start(&db_path(&dir), &["--create"]);
    s.send_line("not json");
    assert_eq!(s.reply()["error"]["code"], -32700);
    assert_eq!(
        s.request("resources/list", json!({}))["error"]["code"],
        -32601
    );
    let no_tool = s.request("tools/call", json!({"name": "drop_everything"}));
    assert_eq!(no_tool["error"]["code"], -32602);
    // A batch (2025-03-26) gets a batch reply, without the notification's.
    s.send_line(
        r#"[{"jsonrpc":"2.0","id":"a","method":"ping"},{"jsonrpc":"2.0","method":"notifications/x"}]"#,
    );
    assert_eq!(
        s.reply(),
        json!([{"jsonrpc": "2.0", "id": "a", "result": {}}])
    );
    // The agent sees what it got wrong, and can fix it.
    let e = s.fail("scan", json!({"table": "t", "limt": 3}));
    assert_eq!(e["code"], "Usage");
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .contains("unknown argument limt"),
        "{e}"
    );
    let e = s.fail("scan", json!({"table": "missing"}));
    assert_eq!(e["code"], "TableNotFound", "{e}");
    let e = s.fail("read_row", json!({"table": "t", "row": 7}));
    assert!(e["message"].as_str().unwrap().contains("row"), "{e}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// create_table's error precedence, pinned across 8c-2's move behind the Backend trait (the
/// coordinator on #1662): the families are parsed before the table is created, which is
/// when its name is checked (`TableBuilder::finish`), so a call with a bad name and a bad
/// family reports the family, as it always has; a bad name alone reports the name.
#[test]
fn create_table_reports_a_bad_family_before_a_bad_name() {
    let dir = TempDir::new("mcp-create-order");
    let mut s = Server::start(&db_path(&dir), &["--create"]);
    let both = s.fail(
        "create_table",
        json!({"table": "", "families": [{"name": "f", "aggregation": "nope"}]}),
    );
    let message = both["message"].as_str().unwrap();
    assert!(message.contains("families[0]"), "{both}");
    let name = s.fail("create_table", json!({"table": "", "families": ["f"]}));
    let message = name["message"].as_str().unwrap();
    assert!(message.contains("empty table name"), "{name}");
}

#[test]
fn a_batch_commits_every_operation_or_none() {
    let dir = TempDir::new("mcp-write");
    let mut s = Server::start(&db_path(&dir), &["--create"]);
    let created = s.call(
        "create_table",
        json!({"table": "mem", "families": ["notes", {"name": "n", "aggregation": "sum"},
                                           {"name": "hi", "aggregation": "max"}]}),
    );
    let fams: Vec<&str> = created["families"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(fams, ["notes", "n", "hi"]);
    let put = |row: &str, q: &str, value: Value| {
        json!({"op": "put", "table": "mem", "row": row, "family": "notes", "qualifier": q,
               "value": value})
    };
    let w = s.call(
        "write",
        json!({"ops": [
            put("u1", "text", json!("hello")),
            put("u1", "age", json!({"i64": 36})),
            put("u1", "ratio", json!({"f64": 0.5})),
            put("u1", "old", json!("x")),
            put("\u{1}bin", "raw", json!({"base64": "/wA="})),
            {"op": "incr", "table": "mem", "row": "u1", "family": "n", "qualifier": "seen",
             "value": 2},
            {"op": "incr", "table": "mem", "row": "u1", "family": "n", "qualifier": "seen",
             "value": {"i64": 3}},
            {"op": "aggregate", "table": "mem", "row": "u1", "family": "hi", "qualifier": "x",
             "value": 9},
            {"op": "aggregate", "table": "mem", "row": "u1", "family": "hi", "qualifier": "x",
             "value": 4},
        ]}),
    );
    assert_eq!(w["committed"], 9, "{w}");
    s.call(
        "write",
        json!({"ops": [{"op": "delete", "table": "mem", "row": "u1", "family": "notes",
                        "qualifier": "old"}]}),
    );
    let row = s.call("read_row", json!({"table": "mem", "row": "u1"}));
    let cells: Vec<(String, String, Value)> = row["cells"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["family"].as_str().unwrap().into(),
                c["qualifier"].as_str().unwrap().into(),
                c["value"].clone(),
            )
        })
        .collect();
    let want = |f: &str, q: &str, v: Value| (f.to_owned(), q.to_owned(), v);
    assert_eq!(
        cells,
        [
            want("notes", "age", json!({"i64": 36})),
            want("notes", "ratio", json!({"f64": 0.5})),
            want("notes", "text", json!("hello")),
            want("n", "seen", json!({"i64": 5})),
            want("hi", "x", json!({"i64": 9})),
        ]
    );
    let one = s.call(
        "read_row",
        json!({"table": "mem", "row": "u1", "families": ["notes"], "qualifier": "text"}),
    );
    assert_eq!(one["cells"].as_array().unwrap().len(), 1, "{one}");
    let bin = s.call("scan", json!({"table": "mem", "prefix": "\u{1}"}));
    assert_eq!(
        bin["rows"][0]["cells"][0]["value"],
        json!({"base64": "/wA="})
    );
    // A bad operation anywhere fails the batch, and nothing in it is written.
    let e = s.fail(
        "write",
        json!({"ops": [put("u2", "a", json!("v")), {"op": "put", "table": "mem", "row": "u2",
                                                    "family": "notes", "qualifier": "b"}]}),
    );
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .starts_with("ops[1]: put needs a value"),
        "{e}"
    );
    let e = s.fail(
        "write",
        json!({"ops": [put("u2", "a", json!("v")), {"op": "put", "table": "mem", "row": "u2",
                       "family": "nope", "qualifier": "b", "value": "v"}]}),
    );
    assert_eq!(e["code"], "FamilyNotFound", "{e}");
    let u2 = s.call("read_row", json!({"table": "mem", "row": "u2"}));
    assert_eq!(u2["cells"], json!([]));

    // An aggregate's input type follows the family's kind, not the JSON number's shape.
    s.call(
        "create_table",
        json!({"table": "agg", "families": [{"name": "hi", "aggregation": "max_f64"},
                                           {"name": "bits", "aggregation": "bit_or"},
                                           {"name": "seen", "aggregation": "hll"},
                                           "plain"]}),
    );
    let agg = |f: &str, v: Value| {
        json!({"op": "aggregate", "table": "agg", "row": "r", "family": f, "qualifier": "q",
               "value": v})
    };
    s.call(
        "write",
        json!({"ops": [agg("hi", json!(3)), agg("hi", json!(2.5)), agg("hi", json!({"i64": 1})),
                       agg("bits", json!(1)), agg("bits", json!({"u64": 4})),
                       agg("seen", json!("ada")), agg("seen", json!(7))]}),
    );
    let hi = s.call(
        "read_row",
        json!({"table": "agg", "row": "r", "families": ["hi"]}),
    );
    assert_eq!(hi["cells"][0]["value"], json!({"f64": 3.0}), "{hi}");
    // Writes of the wrong kind say what to use, in this tool's terms.
    for (op, needle) in [
        (agg("bits", json!(-1)), "non-negative integer"),
        (agg("plain", json!(1)), "use put"),
        (
            json!({"op": "incr", "table": "agg", "row": "r", "family": "hi", "qualifier": "q",
                   "value": 1}),
            "counter (sum) family",
        ),
    ] {
        let e = s.fail("write", json!({"ops": [op]}));
        let m = e["message"].as_str().unwrap();
        assert!(m.contains(needle), "{e}");
        assert!(
            !m.contains("aggregate_f64") && !m.contains("Family::"),
            "{e}"
        );
    }
    let e = s.fail("write", json!({"ops": [agg("nope", json!(1))]}));
    assert_eq!(e["code"], "FamilyNotFound", "{e}");

    // A non-finite float reads back as {"f64_bits"}, which a put takes back.
    let inf = json!({"f64_bits": f64::INFINITY.to_bits()});
    s.call("write", json!({"ops": [put("u3", "x", inf.clone())]}));
    let u3 = s.call("read_row", json!({"table": "mem", "row": "u3"}));
    assert_eq!(u3["cells"][0]["value"], inf);
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");

    // A counter made the 0.1.0 way, a standard family naming D41's add operator, takes incr.
    let db = Pigeonhole::open(db_path(&dir), Options::default().shards(1)).unwrap();
    db.table("old")
        .unwrap()
        .family(
            "n",
            pigeonhole::Family::default().merge_operator("pigeonhole.i64_add"),
        )
        .create()
        .unwrap();
    db.close().unwrap();
    let mut s = Server::start(&db_path(&dir), &[]);
    let incr = json!({"op": "incr", "table": "old", "row": "r", "family": "n",
                      "qualifier": "q", "value": 2});
    // Two commits: in one commit, writes to one cell of a standard family collapse to the
    // last (D34); only counter-kind families add them up (D186).
    s.call("write", json!({"ops": [incr.clone()]}));
    s.call("write", json!({"ops": [incr]}));
    let old = s.call("read_row", json!({"table": "old", "row": "r"}));
    assert_eq!(old["cells"][0]["value"], json!({"i64": 4}), "{old}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    // The writes are in the file after the server closes it.
    let db = Pigeonhole::open(db_path(&dir), Options::default().shards(1)).unwrap();
    let t = db.table("mem").unwrap().open().unwrap();
    let cell = t.get(b"u1", "notes", b"text").unwrap().unwrap();
    assert_eq!(cell.value(), b"hello");
    db.close().unwrap();
}

#[test]
fn long_results_are_cut_with_cursors_that_continue_them() {
    let dir = TempDir::new("mcp-cursors");
    let mut s = Server::start(
        &db_path(&dir),
        &[
            "--create",
            "--max-result-bytes",
            "600",
            "--max-value-bytes",
            "16",
        ],
    );
    s.call(
        "create_table",
        json!({"table": "t", "families": ["f", "g"]}),
    );
    let mut ops = Vec::new();
    for r in 0..40 {
        ops.push(
            json!({"op": "put", "table": "t", "row": format!("r{r:02}"), "family": "f",
                        "qualifier": "q", "value": format!("v{r}")}),
        );
    }
    // One wide row, across two families.
    for c in 0..60 {
        let family = if c < 30 { "f" } else { "g" };
        ops.push(
            json!({"op": "put", "table": "t", "row": "wide", "family": family,
                        "qualifier": format!("c{c:02}"), "value": "x"}),
        );
    }
    ops.push(
        json!({"op": "put", "table": "t", "row": "big", "family": "f", "qualifier": "q",
                    "value": "a value longer than sixteen bytes"}),
    );
    s.call("write", json!({"ops": ops}));

    // A scan pages through every row exactly once.
    let (rows, calls) = pages(&mut s, "scan", json!({"table": "t", "prefix": "r"}), "rows");
    let keys: Vec<String> = rows
        .iter()
        .map(|r| r["row"].as_str().unwrap().into())
        .collect();
    let want: Vec<String> = (0..40).map(|r| format!("r{r:02}")).collect();
    assert_eq!(keys, want);
    assert!(calls > 3, "{calls} calls: the limit did not cut");
    // A client that re-sends omitted arguments as null keeps its cursor.
    let first = s.call("scan", json!({"table": "t", "prefix": "r"}));
    let again = s.call(
        "scan",
        json!({"table": "t", "prefix": "r", "limit": null, "families": null,
               "cursor": first["cursor"]}),
    );
    assert_eq!(
        again["rows"][0]["row"],
        rows[first["rows"].as_array().unwrap().len()]["row"]
    );
    // So does the row limit.
    let (rows, calls) = pages(
        &mut s,
        "scan",
        json!({"table": "t", "start": "r10", "end": "r20", "limit": 4}),
        "rows",
    );
    assert_eq!((rows.len(), calls), (10, 3));

    // A wide row pages by columns, each once, across the family change.
    let (cells, calls) = pages(
        &mut s,
        "read_row",
        json!({"table": "t", "row": "wide"}),
        "cells",
    );
    let cols: Vec<String> = cells
        .iter()
        .map(|c| c["qualifier"].as_str().unwrap().into())
        .collect();
    let want: Vec<String> = (0..60).map(|c| format!("c{c:02}")).collect();
    assert_eq!(cols, want);
    assert!(calls > 3, "{calls}");
    // In a scan, a row too big for the result alone comes back cut and marked.
    let wide = s.call("scan", json!({"table": "t", "prefix": "wide"}));
    assert_eq!(wide["rows"][0]["truncated"], true, "{wide}");
    let rest = s.call(
        "scan",
        json!({"table": "t", "prefix": "wide", "cursor": wide["cursor"]}),
    );
    assert_eq!(rest["rows"], json!([]), "{rest}");
    assert!(rest.get("cursor").is_none());

    // A long value is left out, unless the read asks for it.
    let big = s.call("read_row", json!({"table": "t", "row": "big"}));
    assert_eq!(big["cells"][0]["value"], json!({"omitted_bytes": 33}));
    let big = s.call(
        "read_row",
        json!({"table": "t", "row": "big", "max_value_bytes": 100}),
    );
    assert_eq!(
        big["cells"][0]["value"],
        "a value longer than sixteen bytes"
    );

    // A cursor only continues the call that returned it.
    let first = s.call("read_row", json!({"table": "t", "row": "wide"}));
    let e = s.fail(
        "read_row",
        json!({"table": "t", "row": "wide", "families": ["g"], "cursor": first["cursor"]}),
    );
    assert!(
        e["message"].as_str().unwrap().contains("other arguments"),
        "{e}"
    );
    let e = s.fail(
        "scan",
        json!({"table": "t", "row": "wide", "cursor": first["cursor"]}),
    );
    assert_eq!(e["code"], "Usage", "{e}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

#[test]
fn traverse_follows_edges_hop_by_hop() {
    let dir = TempDir::new("mcp-traverse");
    let mut s = Server::start(&db_path(&dir), &["--create", "--max-result-bytes", "200"]);
    s.call("create_table", json!({"table": "g", "families": ["out"]}));
    let edge = |a: &str, b: &str| {
        json!({"op": "put", "table": "g", "row": format!("n:{a}"), "family": "out",
               "qualifier": format!("n:{b}"), "value": ""})
    };
    let mut ops = vec![
        edge("a", "b"),
        edge("a", "c"),
        edge("b", "d"),
        edge("c", "d"),
        edge("d", "e"),
        edge("d", "a"),
    ];
    for i in 0..30 {
        ops.push(edge("e", &format!("leaf{i:02}")));
    }
    s.call("write", json!({"ops": ops}));
    let t = s.call(
        "traverse",
        json!({"table": "g", "family": "out", "seeds": ["n:a"], "hops": 3, "parents": true}),
    );
    let hops = t["hops"].as_array().unwrap();
    assert_eq!(
        hops[0]["nodes"],
        json!([{"node": "n:b", "parent": "n:a"}, {"node": "n:c", "parent": "n:a"}])
    );
    assert_eq!(hops[1]["nodes"], json!([{"node": "n:d", "parent": "n:b"}]));
    assert_eq!(hops[2]["nodes"], json!([{"node": "n:e", "parent": "n:d"}]));
    assert!(t.get("cursor").is_none(), "{t}");

    // A fourth hop reaches the 30 leaves, past the result limit: the cursor goes on.
    let args = json!({"table": "g", "family": "out", "seeds": ["n:a"], "hops": 4});
    let mut nodes = Vec::new();
    let mut args_now = args.clone();
    let mut calls = 0;
    loop {
        let out = s.call("traverse", args_now.clone());
        calls += 1;
        for h in out["hops"].as_array().unwrap() {
            nodes.extend(h["nodes"].as_array().unwrap().iter().cloned());
        }
        match out.get("cursor") {
            Some(c) => args_now["cursor"] = c.clone(),
            None => break,
        }
    }
    assert!(calls > 1);
    assert_eq!(nodes.len(), 4 + 30, "{nodes:?}");
    let leaves: Vec<&str> = nodes[4..].iter().map(|n| n.as_str().unwrap()).collect();
    let want: Vec<String> = (0..30).map(|i| format!("n:leaf{i:02}")).collect();
    assert_eq!(leaves, want);
    let capped = s.call(
        "traverse",
        json!({"table": "g", "family": "out", "seeds": ["n:a"], "per_node_limit": 1}),
    );
    assert_eq!(capped["hops"][0]["nodes"], json!(["n:b"]));
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// #1254: a traversal's cursors page from the server's cached result, so writes between
/// pages change nothing: every node of the first result comes back exactly once, and none
/// added later. (Re-running the traversal, as before, would shift indexes: the edges added
/// below sort among the first page's nodes, so the next page would repeat one.) A cursor
/// whose run isn't cached any more (another traversal ran) runs it again.
#[test]
fn traverse_pages_come_from_one_cached_result() {
    let dir = TempDir::new("mcp-traverse-cache");
    let mut s = Server::start(&db_path(&dir), &["--create", "--max-result-bytes", "200"]);
    s.call("create_table", json!({"table": "g", "families": ["out"]}));
    let edge = |b: &str| {
        json!({"op": "put", "table": "g", "row": "n:hub", "family": "out",
               "qualifier": format!("n:{b}"), "value": ""})
    };
    let ops: Vec<Value> = (0..60).map(|i| edge(&format!("{:03}", i * 10))).collect();
    s.call("write", json!({"ops": ops}));
    let want: Vec<String> = (0..60).map(|i| format!("n:{:03}", i * 10)).collect();
    let args = json!({"table": "g", "family": "out", "seeds": ["n:hub"]});

    let first = s.call("traverse", args.clone());
    let mut got: Vec<String> = first["hops"][0]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_str().unwrap().to_owned())
        .collect();
    let mut cursor = first["cursor"].clone();
    assert!(cursor.is_string(), "{first}");
    let mut wrote = 0;
    while cursor.is_string() {
        // A writer adds an edge that sorts before the next page's first node.
        let ops: Vec<Value> = (0..3)
            .map(|j| edge(&format!("{:03}", wrote * 3 + j + 1)))
            .collect();
        s.call("write", json!({"ops": ops}));
        wrote += 1;
        let mut next = args.clone();
        next["cursor"] = cursor;
        let page = s.call("traverse", next);
        got.extend(
            page["hops"][0]["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap().to_owned()),
        );
        cursor = page.get("cursor").cloned().unwrap_or(Value::Null);
        assert!(wrote < 100, "cursors never end");
    }
    assert!(wrote > 1, "the result fit one page");
    assert_eq!(
        got, want,
        "every node of the first result once, none added later"
    );

    // A traversal that fits one result leaves the cache alone; one that returns a cursor
    // replaces it, and then an old cursor runs its traversal again, as cursors did before
    // the cache: it sees an edge added since its first page.
    let second = s.call("traverse", args.clone());
    let stale = second["cursor"].clone();
    let counted = second["hops"][0]["node_count"].as_u64().unwrap();
    s.call("write", json!({"ops": [edge("999")]}));
    s.call(
        "traverse",
        json!({"table": "g", "family": "out", "seeds": ["n:000"]}),
    );
    let mut cached = args.clone();
    cached["cursor"] = stale.clone();
    let page = s.call("traverse", cached);
    assert_eq!(
        page["hops"][0]["node_count"].as_u64(),
        Some(counted),
        "{page}"
    );
    let other = s.call(
        "traverse",
        json!({"table": "g", "family": "out", "seeds": ["n:hub"], "per_node_limit": 1000}),
    );
    assert!(other["cursor"].is_string(), "{other}");
    let mut again = args.clone();
    again["cursor"] = stale;
    let page = s.call("traverse", again);
    assert!(
        page["hops"][0]["first_index"].as_u64().unwrap() > 0,
        "{page}"
    );
    assert_eq!(
        page["hops"][0]["node_count"].as_u64(),
        Some(counted + 1),
        "the re-run sees the edge added since: {page}"
    );
    // The same call run again caches a newer run: a cursor from the older run doesn't
    // page from it (it carries its run id) but runs again, seeing what's newer still.
    let older = s.call("traverse", args.clone());
    let older_cursor = older["cursor"].clone();
    let base = older["hops"][0]["node_count"].as_u64().unwrap();
    s.call("write", json!({"ops": [edge("998")]}));
    let newer = s.call("traverse", args.clone());
    assert_eq!(newer["hops"][0]["node_count"].as_u64(), Some(base + 1));
    s.call("write", json!({"ops": [edge("997")]}));
    let mut again = args.clone();
    again["cursor"] = older_cursor;
    let page = s.call("traverse", again);
    assert_eq!(
        page["hops"][0]["node_count"].as_u64(),
        Some(base + 2),
        "an older run's cursor runs again, not from the newer run's cache: {page}"
    );
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

#[test]
fn read_only_serves_a_live_database_as_a_reader_without_write_tools() {
    let dir = TempDir::new("mcp-reader");
    let path = db_path(&dir);
    // No writer running: a reader process can't attach.
    Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false))
        .unwrap()
        .close()
        .unwrap();
    let (status, err) = Server::spawn(&path, &["--read-only"]).finish();
    assert_eq!(status, 4, "{err}");
    assert!(err.contains("NoWriter") || err.contains("writer"), "{err}");

    // The writer feeds (D310), so the reader serves feed_poll too (#1361).
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .create_if_missing(true)
            .change_feed(pigeonhole::FeedRetention::bytes(16 << 20)),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", pigeonhole::Family::default())
        .create()
        .unwrap();
    t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();
    let mut s = Server::start(&path, &["--read-only"]);
    assert_eq!(
        s.tool_names(),
        ["list_tables", "read_row", "scan", "traverse", "feed_poll"]
    );
    // A reader process follows the writer's feed: a first poll's token, then the writer's
    // next commit arrives from it.
    let first = s.call("feed_poll", json!({"tables": ["t"]}));
    assert_eq!(first["commits"], json!([]), "{first}");
    t.mutate(b"fed").put("f", b"q", b"x").commit().unwrap();
    let fed = s.call(
        "feed_poll",
        json!({"tables": ["t"], "token": first["token"], "wait_ms": 5000}),
    );
    assert_eq!(fed["commits"][0]["changes"][0]["row"], "fed", "{fed}");
    let tables = s.call("list_tables", json!({}));
    assert_eq!(tables["mode"], "reader");
    assert_eq!(tables["writable"], false);
    let row = s.call("read_row", json!({"table": "t", "row": "r"}));
    assert_eq!(row["cells"][0]["value"], "v");
    // The writer's later commits are visible.
    t.mutate(b"r").put("f", b"q2", b"w").commit().unwrap();
    let row = s.call(
        "read_row",
        json!({"table": "t", "row": "r", "qualifier": "q2"}),
    );
    assert_eq!(row["cells"][0]["value"], "w", "{row}");
    let e = s.fail(
        "write",
        json!({"ops": [{"op": "delete", "table": "t", "row": "r"}]}),
    );
    assert_eq!(e["code"], "ReadOnly", "{e}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    db.close().unwrap();

    // Without --read-only, phdb mcp beside a running writer is a reader too, with no write
    // tools (feed_poll is listed on any reader open, #1361).
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let mut s = Server::start(&path, &[]);
    assert_eq!(
        s.tool_names(),
        ["list_tables", "read_row", "scan", "traverse", "feed_poll"]
    );
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    db.close().unwrap();
}

/// Vector families through the read and write tools (D303, #1309): `list_tables` shows
/// the spec, `read_row` returns vectors in `dump`'s shape (or their shape, when long), and
/// `write` puts one in that shape, checked against the family.
#[test]
fn vector_families_read_and_write_in_dump_shapes() {
    use pigeonhole::{Element, Metric, VectorSpec};
    let dir = TempDir::new("mcp-vectors");
    let path = db_path(&dir);
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let t = db
        .table("docs")
        .unwrap()
        .family(
            "emb",
            pigeonhole::Family::vector(VectorSpec::new(4, Element::F32, Metric::Cosine).unwrap()),
        )
        .create()
        .unwrap();
    t.mutate(b"d1")
        .put_vector_f32("emb", b"v", &[0.5, -1.0, 2.0, 0.25])
        .commit()
        .unwrap();
    drop(t);
    db.close().unwrap();

    let mut s = Server::start(&path, &[]);
    let tables = s.call("list_tables", json!({}));
    assert_eq!(
        tables["tables"][0]["families"][0]["vector"],
        json!({"dim": 4, "element": "f32", "metric": "cosine"}),
        "{tables}"
    );
    let row = s.call("read_row", json!({"table": "docs", "row": "d1"}));
    assert_eq!(
        row["cells"][0]["value"],
        json!({"vector": {"dim": 4, "element": "f32", "values": [0.5, -1.0, 2.0, 0.25]}}),
        "{row}"
    );
    // A payload over max_value_bytes comes back as its shape.
    let short = s.call(
        "read_row",
        json!({"table": "docs", "row": "d1", "max_value_bytes": 8}),
    );
    assert_eq!(
        short["cells"][0]["value"],
        json!({"omitted_vector": {"element": "f32", "dim": 4}}),
        "{short}"
    );
    // write takes the same shape, checked against the family.
    let put = |values: Value, dim: u64| {
        json!({"ops": [{"op": "put", "table": "docs", "row": "d2", "family": "emb",
                        "qualifier": "v", "value": {"vector": {"dim": dim, "element": "f32",
                                                               "values": values}}}]})
    };
    s.call("write", put(json!([1.0, 0.0, 0.0, 0.0]), 4));
    let d2 = s.call("read_row", json!({"table": "docs", "row": "d2"}));
    assert_eq!(
        d2["cells"][0]["value"]["vector"]["values"],
        json!([1.0, 0.0, 0.0, 0.0])
    );
    let err = s.fail("write", put(json!([1.0, 0.0]), 2));
    assert!(err.to_string().contains("f32[4]"), "{err}");
    // Under cosine, an all-zero query is the agent's mistake, refused before the search.
    let err = s.fail(
        "nearest",
        json!({"table": "docs", "family": "emb", "query": [0.0, 0.0, 0.0, 0.0]}),
    );
    assert!(err.to_string().contains("no direction"), "{err}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// `nearest` (#1253): listed once the database has a vector family; the k nearest, in
/// order, under the read's selection; queries checked against the family; bit queries as
/// 0s and 1s or packed base64; a result past the size cap cut at a neighbour.
#[test]
fn nearest_ranks_vectors_under_the_selection() {
    use pigeonhole::{Element, Family, Metric, VectorSpec};
    let dir = TempDir::new("mcp-nearest");
    let path = db_path(&dir);
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let t = db
        .table("docs")
        .unwrap()
        .family(
            "emb",
            Family::vector(VectorSpec::new(2, Element::F32, Metric::L2).unwrap()),
        )
        .family(
            "sig",
            Family::vector(VectorSpec::new(16, Element::Bit, Metric::Hamming).unwrap()),
        )
        .family("note", Family::default())
        .create()
        .unwrap();
    for (row, v, bits) in [
        (b"a", [0.0, 0.0], [0b0000_0000u8, 0b00]),
        (b"b", [1.0, 0.0], [0b0000_0011, 0b10]),
        (b"c", [5.0, 5.0], [0b1111_1111, 0b11]),
    ] {
        t.mutate(row)
            .put_vector_f32("emb", b"v", &v)
            .put_vector_bits("sig", b"s", &bits)
            .commit()
            .unwrap();
    }
    drop(t);
    db.close().unwrap();

    let mut s = Server::start(&path, &[]);
    let names = s.tool_names();
    assert_eq!(
        names[..5],
        ["list_tables", "read_row", "scan", "traverse", "nearest"]
    );
    let rows = |r: &Value| -> Vec<String> {
        r["neighbours"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["row"].as_str().unwrap().to_owned())
            .collect()
    };
    let near = s.call(
        "nearest",
        json!({"table": "docs", "family": "emb", "query": [0.9, 0.0], "k": 2}),
    );
    assert_eq!(rows(&near), ["b", "a"], "{near}");
    assert_eq!(
        near["vector"],
        json!({"dim": 2, "element": "f32", "metric": "l2"})
    );
    let n = &near["neighbours"];
    assert_eq!(n[0]["qualifier"], "v");
    assert!(n[0]["ts"].as_u64().unwrap() > 0);
    assert!(n[0]["distance"].as_f64().unwrap() < n[1]["distance"].as_f64().unwrap());
    assert!(near.get("truncated").is_none());
    // The selection: a prefix, one row; write's vector shape as the query.
    let c = s.call(
        "nearest",
        json!({"table": "docs", "family": "emb", "query": [0.0, 0.0], "prefix": "c"}),
    );
    assert_eq!(rows(&c), ["c"], "{c}");
    let a = s.call(
        "nearest",
        json!({"table": "docs", "family": "emb", "row": "a", "qualifier": "v",
               "query": {"vector": {"dim": 2, "element": "f32", "values": [5.0, 5.0]}}}),
    );
    assert_eq!(rows(&a), ["a"], "{a}");
    // Bits: 0s and 1s, least significant first, or the packed bytes.
    let bits = s.call(
        "nearest",
        json!({"table": "docs", "family": "sig", "k": 3,
               "query": [1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]}),
    );
    assert_eq!(rows(&bits), ["b", "a", "c"], "{bits}");
    let packed = s.call(
        "nearest",
        json!({"table": "docs", "family": "sig", "k": 3, "query": {"base64": "AwI="}}),
    );
    assert_eq!(packed["neighbours"], bits["neighbours"]);
    // Refusals name what the family takes.
    for (args, want) in [
        (json!({"query": [1.0]}), "f32[2]"),
        (json!({"query": [1.0, "x"]}), "finite f32"),
        (
            json!({"family": "note", "query": [1.0, 0.0]}),
            "not a vector family",
        ),
        (
            json!({"family": "sig", "query": [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}),
            "0 or 1",
        ),
        (
            json!({"query": [1.0, 0.0], "row": "a", "prefix": "a"}),
            "not two of them",
        ),
        (json!({"query": [1.0, 0.0], "k": 0}), "k:"),
        (json!({"query": [1e30, 0.0]}), "2^56"),
    ] {
        let mut call = json!({"table": "docs", "family": "emb"});
        for (k, v) in args.as_object().unwrap() {
            call[k] = v.clone();
        }
        let err = s.fail("nearest", call);
        assert!(err.to_string().contains(want), "{want}: {err}");
    }
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");

    // Past the size cap: the nearest that fit, marked truncated (a ranking can't resume).
    let mut s = Server::start(&path, &["--max-result-bytes", "200"]);
    let cut = s.call(
        "nearest",
        json!({"table": "docs", "family": "emb", "query": [0.9, 0.0], "k": 3}),
    );
    assert_eq!(cut["truncated"], true, "{cut}");
    let kept = rows(&cut);
    assert!(!kept.is_empty() && kept.len() < 3, "{cut}");
    assert_eq!(kept[0], "b");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// `feed_poll` (#1322): listed when the database's feed is on (`--change-feed`, or inherited
/// from the last open, D310 amendment (c)); whole commits in order after a token, the next
/// token continuing exactly; `max_commits` with `more`; and a token whose history an open
/// with the feed off ended is a `FeedExpired` tool error.
#[test]
fn feed_poll_continues_from_its_token_and_reports_expiry() {
    let dir = TempDir::new("mcp-feed");
    let path = db_path(&dir);
    let feed = ["--change-feed", "16777216"];
    let mut s = Server::start(&path, &["--create", feed[0], feed[1]]);
    assert!(s.tool_names().contains(&"feed_poll".to_owned()));
    s.call(
        "create_table",
        json!({"table": "t", "families": ["f", {"name": "n", "aggregation": "sum"}]}),
    );
    let first = s.call("feed_poll", json!({"tables": ["t"]}));
    assert_eq!(first["commits"], json!([]), "{first}");
    let t0 = first["token"].clone();
    let put = |row: &str| {
        json!({"ops": [{"op": "put", "table": "t", "row": row, "family": "f",
                        "qualifier": "q", "value": "v"}]})
    };
    s.call("write", put("r1"));
    s.call(
        "write",
        json!({"ops": [
            {"op": "incr", "table": "t", "row": "r1", "family": "n", "qualifier": "c", "value": 2},
            {"op": "delete", "table": "t", "row": "r1", "family": "f", "qualifier": "q"},
        ]}),
    );
    let got = s.call(
        "feed_poll",
        json!({"tables": ["t"], "token": t0, "wait_ms": 5000}),
    );
    let commits = got["commits"].as_array().unwrap();
    assert_eq!(commits.len(), 2, "{got}");
    assert_eq!(
        commits[0]["changes"],
        json!([{"table": "t", "family": "f", "kind": "put", "row": "r1", "qualifier": "q",
                "ts": commits[0]["changes"][0]["ts"], "value": "v"}])
    );
    let kinds: Vec<&str> = commits[1]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["merge", "delete_column"], "{got}");
    assert!(commits[1]["changes"][1].get("value").is_none());
    assert!(commits[1]["seqno"].as_u64() > commits[0]["seqno"].as_u64());
    let t1 = got["token"].clone();
    // The token continues exactly: nothing new yet.
    let again = s.call("feed_poll", json!({"tables": ["t"], "token": t1}));
    assert_eq!(again["commits"], json!([]), "{again}");
    // max_commits, with `more`, and the token after the last returned.
    for r in ["a", "b", "c"] {
        s.call("write", put(r));
    }
    let two = s.call(
        "feed_poll",
        json!({"tables": ["t"], "token": t1, "max_commits": 2, "wait_ms": 5000}),
    );
    assert_eq!(two["commits"].as_array().unwrap().len(), 2, "{two}");
    assert_eq!(two["more"], true, "{two}");
    let rest = s.call(
        "feed_poll",
        json!({"tables": ["t"], "token": two["token"], "wait_ms": 5000}),
    );
    assert_eq!(rest["commits"].as_array().unwrap().len(), 1, "{rest}");
    assert_eq!(rest["commits"][0]["changes"][0]["row"], "c");
    assert!(rest.get("more").is_none());
    let kept = rest["token"].clone();
    // Refusals.
    let err = s.fail("feed_poll", json!({"tables": ["t"], "token": "!!"}));
    assert!(err.to_string().contains("token"), "{err}");
    let err = s.fail(
        "feed_poll",
        json!({"tables": ["t"], "token": kept, "from_oldest": true}),
    );
    assert!(err.to_string().contains("not both"), "{err}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");

    // A server without --change-feed inherits the feed the last open had: listed, and the
    // token continues.
    let mut s = Server::start(&path, &[]);
    assert!(s.tool_names().contains(&"feed_poll".to_owned()));
    let same = s.call("feed_poll", json!({"tables": ["t"], "token": kept}));
    assert_eq!(same["commits"], json!([]), "{same}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");

    // An open that turns the feed off ends its history: the token expired.
    Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .change_feed_off(),
    )
    .unwrap()
    .close()
    .unwrap();
    let mut s = Server::start(&path, &feed);
    let err = s.fail("feed_poll", json!({"tables": ["t"], "token": kept}));
    assert_eq!(err["code"], "FeedExpired", "{err}");
    assert_eq!(err["code_num"], 31, "{err}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// #1356: the server advertises `listChanged`, and once the client has listed the tools, a
/// call that changes them (the first vector family brings `nearest`) is followed by
/// `notifications/tools/list_changed`, once; a call that changes nothing sends none.
#[test]
fn a_new_vector_family_announces_nearest() {
    let dir = TempDir::new("mcp-list-changed");
    let mut s = Server::spawn(&db_path(&dir), &["--create"]);
    let init = s.request("initialize", json!({"protocolVersion": "2025-06-18"}));
    assert_eq!(init["result"]["capabilities"]["tools"]["listChanged"], true);
    s.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    let names = s.tool_names();
    assert!(!names.contains(&"nearest".to_owned()), "{names:?}");
    // A plain table changes nothing: no notification.
    s.call("create_table", json!({"table": "plain", "families": ["f"]}));
    s.call("list_tables", json!({}));
    assert!(s.notes.is_empty(), "{:?}", s.notes);
    // The first vector family: the reply, then the notification.
    s.call(
        "create_table",
        json!({"table": "docs", "families": [
            {"name": "emb", "vector": {"dim": 2, "element": "f32", "metric": "l2"}}
        ]}),
    );
    s.call("list_tables", json!({}));
    assert_eq!(
        s.notes,
        [json!({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})]
    );
    assert!(s.tool_names().contains(&"nearest".to_owned()));
    // Once per change.
    s.call("list_tables", json!({}));
    assert_eq!(s.notes.len(), 1);
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// The memory tools' names, as `tools/list` gives them on a writable server.
const MEMORY_TOOLS: [&str; 6] = [
    "memory_recall",
    "memory_remember",
    "memory_mark_used",
    "memory_supersede",
    "memory_link",
    "memory_forget",
];

/// `--memory` on a store without an embedding model (D311 T7): the six tools, the
/// instructions and the guide resource, and each tool's round trip.
#[test]
fn memory_tools_remember_recall_supersede_and_forget() {
    let dir = TempDir::new("mcp-memory");
    let path = db_path(&dir);
    let (mut s, init) = Server::start_env(&path, &["--create", "--memory", "agent"], &[]);
    let instructions = init["instructions"].as_str().unwrap();
    assert!(instructions.contains("Long-term memory"), "{instructions}");
    assert!(instructions.contains("memory://guide"), "{instructions}");
    assert!(init["capabilities"]["resources"].is_object(), "{init}");
    let names = s.tool_names();
    assert!(
        names.ends_with(&MEMORY_TOOLS.map(String::from)),
        "{names:?}"
    );
    let list = s.request("tools/list", json!({}));
    for t in list["result"]["tools"].as_array().unwrap() {
        let name = t["name"].as_str().unwrap();
        if name.starts_with("memory_") {
            assert!(t["description"].as_str().unwrap().len() > 150, "{t}");
            assert_eq!(
                t["annotations"]["destructiveHint"],
                name == "memory_forget",
                "{t}"
            );
            assert_eq!(t["annotations"]["readOnlyHint"], name == "memory_recall");
        }
    }
    // The guide resource.
    let res = s.request("resources/list", json!({}));
    assert_eq!(res["result"]["resources"][0]["uri"], "memory://guide");
    let guide = s.request("resources/read", json!({"uri": "memory://guide"}));
    let text = guide["result"]["contents"][0]["text"].as_str().unwrap();
    assert!(text.starts_with("# Using the memory tools"), "{text}");
    let missing = s.request("resources/read", json!({"uri": "memory://nope"}));
    assert_eq!(missing["error"]["code"], -32002, "{missing}");

    // Remember; an exact duplicate (after whitespace) merges.
    let tabs = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana prefers tabs in Go code", "kind": "semantic", "importance": 0.9}),
    );
    assert_eq!(tabs["outcome"], "new", "{tabs}");
    let id = tabs["id"].as_str().unwrap().to_owned();
    let again = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "  Dana prefers tabs   in Go code"}),
    );
    assert_eq!(
        (&again["outcome"], &again["id"]),
        (&json!("merged"), &json!(id))
    );
    s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana's team ships on Thursdays"}),
    );
    // Another namespace is never recalled.
    s.call(
        "memory_remember",
        json!({"namespace": "user:sam", "text": "Sam prefers tabs too"}),
    );
    let hits = s.call(
        "memory_recall",
        json!({"namespace": "user:dana", "text": "tabs"}),
    );
    assert_eq!(hits["hits"][0]["id"], json!(id), "{hits}");
    assert_eq!(hits["hits"][0]["kind"], "semantic");
    assert_eq!(hits["hits"].as_array().unwrap().len(), 2, "{hits}");
    assert_eq!(hits["truncated"], false);

    // A changed fact supersedes the old one: recall leaves the old one out.
    let spaces = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana now prefers spaces in Go code", "supersedes": id}),
    );
    assert_eq!(spaces["superseded"], true, "{spaces}");
    let new_id = spaces["id"].as_str().unwrap().to_owned();
    let hits = s.call(
        "memory_recall",
        json!({"namespace": "user:dana", "text": "prefers Go code"}),
    );
    let ids: Vec<&str> = hits["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&new_id.as_str()) && !ids.contains(&id.as_str()),
        "{hits}"
    );
    let all = s.call(
        "memory_recall",
        json!({"namespace": "user:dana", "text": "tabs", "include_superseded": true}),
    );
    assert_eq!(all["hits"][0]["superseded_by"], json!(new_id), "{all}");

    // A failed supersede still returns the stored memory (simp724's S2): c is superseded by
    // d already, so e's supersede fails, and e is stored all the same.
    let c = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana uses vim"}),
    )["id"]
        .clone();
    let d = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana uses helix"}),
    )["id"]
        .clone();
    s.call("memory_supersede", json!({"old": c, "new": d}));
    let e = s.call(
        "memory_remember",
        json!({"namespace": "user:dana", "text": "Dana uses zed", "supersedes": c}),
    );
    assert_eq!(
        (&e["outcome"], &e["superseded"]),
        (&json!("new"), &json!(false)),
        "{e}"
    );
    assert!(e["supersede_error"]["message"].as_str().is_some(), "{e}");
    let zed = s.call(
        "memory_recall",
        json!({"namespace": "user:dana", "text": "zed"}),
    );
    assert_eq!(zed["hits"][0]["id"], e["id"], "{zed}");
    // A memory supersedes one of its own namespace only, refused before the write.
    let e = s.fail(
        "memory_remember",
        json!({"namespace": "user:sam", "text": "Sam uses emacs", "supersedes": d}),
    );
    assert!(e["message"].as_str().unwrap().contains("namespace"), "{e}");
    let emacs = s.call(
        "memory_recall",
        json!({"namespace": "user:sam", "text": "emacs"}),
    );
    assert!(
        emacs["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|h| h["text"] != "Sam uses emacs"),
        "{emacs}"
    );

    // Use, links, and forgetting.
    let used = s.call("memory_mark_used", json!({"ids": [new_id]}));
    assert_eq!(used, json!({"marked": 1, "missing": []}));
    s.call(
        "memory_link",
        json!({"from": new_id, "to": id, "type": "derived_from", "weight": 0.5}),
    );
    let e = s.fail(
        "memory_link",
        json!({"from": new_id, "to": id, "type": "supersedes"}),
    );
    assert!(
        e["message"].as_str().unwrap().contains("memory_supersede"),
        "{e}"
    );
    let sam = s.call(
        "memory_recall",
        json!({"namespace": "user:sam", "text": "tabs"}),
    )["hits"][0]["id"]
        .clone();
    let e = s.fail("memory_link", json!({"from": new_id, "to": sam}));
    assert_eq!(e["code"], "InvalidArgument", "{e}");
    let gone = s.call("memory_forget", json!({"ids": [id]}));
    assert_eq!(gone, json!({"deleted": 1, "missing": []}));
    let gone = s.call("memory_forget", json!({"ids": [id]}));
    assert_eq!(gone, json!({"deleted": 0, "missing": [id]}));
    let used = s.call("memory_mark_used", json!({"ids": [id]}));
    assert_eq!(used, json!({"marked": 0, "missing": [id]}));

    // Mistakes are tool errors.
    for (tool, args) in [
        ("memory_remember", json!({"text": "x", "kind": "dream"})),
        ("memory_remember", json!({"text": "x", "importance": 2})),
        ("memory_remember", json!({"text": "x", "embedding": [1.0]})),
        ("memory_remember", json!({"text": "x", "colour": "red"})),
        ("memory_recall", json!({})),
        (
            "memory_recall",
            json!({"text": "x", "namespace": "a", "namespaces": ["b"]}),
        ),
        ("memory_forget", json!({"ids": []})),
        ("memory_forget", json!({"ids": ["not an id"]})),
        (
            "memory_supersede",
            json!({"old": "not an id", "new": new_id}),
        ),
    ] {
        s.fail(tool, args);
    }
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    // `phdb mcp`'s startup line; `phdb serve` logs the store's creation its own way.
    if !transport::http() {
        assert!(
            err.contains("created memory store agent (no embedding model"),
            "{err}"
        );
    }

    // Reopened, the store is there; a model now is refused, since it has none.
    let mut s = Server::start(&path, &["--memory", "agent"]);
    let hits = s.call(
        "memory_recall",
        json!({"namespace": "user:dana", "text": "spaces"}),
    );
    assert_eq!(hits["hits"][0]["id"], json!(new_id), "{hits}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    let (status, err) = Server::spawn(
        &path,
        &[
            "--memory",
            "agent",
            "--memory-model",
            "m1",
            "--memory-dim",
            "4",
        ],
    )
    .finish();
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("has no embedding model"), "{err}");
}

/// A store pinned to a client's model (part 2 of the owner's T7 decision): an embedding must
/// name the store's model and fit its dimension, and another model is refused at startup.
#[test]
fn a_pinned_store_refuses_another_models_embeddings() {
    let dir = TempDir::new("mcp-memory-pinned");
    let path = db_path(&dir);
    let flags = [
        "--create",
        "--memory",
        "agent",
        "--memory-model",
        "m1",
        "--memory-dim",
        "4",
        "--memory-element",
        "f16",
    ];
    let (mut s, init) = Server::start_env(&path, &flags, &[]);
    assert!(
        init["instructions"]
            .as_str()
            .unwrap()
            .contains("\"m1\" (4 dimensions)"),
        "{init}"
    );
    let e = s.fail("memory_remember", json!({"text": "no vector"}));
    assert!(e["message"].as_str().unwrap().contains("pinned"), "{e}");
    let v = json!([1.0, 0.0, 0.0, 0.0]);
    for args in [
        json!({"text": "x", "embedding": v}),
        json!({"text": "x", "embedding": v, "embedding_model": "m2"}),
        json!({"text": "x", "embedding": [1.0, 0.0, 0.0], "embedding_model": "m1"}),
        json!({"text": "x", "embedding": [1.0, "a", 0.0, 0.0], "embedding_model": "m1"}),
        json!({"text": "x", "embedding_model": "m1"}),
    ] {
        s.fail("memory_remember", args);
    }
    let a = s.call(
        "memory_remember",
        json!({"text": "the deploy key lives in vault", "embedding": v, "embedding_model": "m1", "importance": 0.9}),
    );
    let b = s.call(
        "memory_remember",
        json!({"text": "lunch is at noon", "embedding": [0.0, 1.0, 0.0, 0.0], "embedding_model": "m1"}),
    );
    // By embedding alone: the vector rank decides between a (more important) and b (newer).
    // Then by text alone (keyword recall on a pinned store).
    for (query, want) in [([0.9, 0.1, 0.0, 0.0], &a), ([0.1, 0.9, 0.0, 0.0], &b)] {
        let hits = s.call(
            "memory_recall",
            json!({"embedding": query, "embedding_model": "m1"}),
        );
        assert_eq!(hits["hits"][0]["id"], want["id"], "{query:?}: {hits}");
    }
    let hits = s.call("memory_recall", json!({"text": "lunch"}));
    assert_eq!(hits["hits"][0]["id"], b["id"], "{hits}");
    s.fail(
        "memory_recall",
        json!({"embedding": [0.9, 0.1, 0.0, 0.0], "embedding_model": "m2"}),
    );
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    // `phdb mcp`'s startup line; `phdb serve` logs the store's creation its own way.
    if !transport::http() {
        assert!(err.contains("embedding model m1, 4 dimensions"), "{err}");
    }

    // Reopened without a model flag: the pinned one is used. With another: refused.
    let (s, init) = Server::start_env(&path, &["--memory", "agent"], &[]);
    assert!(init["instructions"].as_str().unwrap().contains("\"m1\""));
    assert_eq!(s.finish().0, 0);
    for flags in [
        &[
            "--memory",
            "agent",
            "--memory-model",
            "m2",
            "--memory-dim",
            "4",
        ][..],
        &[
            "--memory",
            "agent",
            "--memory-model",
            "m1",
            "--memory-dim",
            "8",
        ][..],
    ] {
        let (status, err) = Server::spawn(&path, flags).finish();
        assert_eq!(status, 2, "{err}");
        assert!(err.contains("pinned"), "{err}");
    }
}

/// `--memory-normalization` (#1405): a new store's text-index normalisation, fixed at
/// create. Reopened, the flag may repeat it or be left out; another value fails at startup.
#[test]
fn a_new_stores_normalization_is_fixed_and_checked_at_startup() {
    let dir = TempDir::new("mcp-normalization");
    let path = db_path(&dir);
    let (s, _) = Server::start_env(
        &path,
        &[
            "--create",
            "--memory",
            "agent",
            "--memory-normalization",
            "nfkc",
        ],
        &[],
    );
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    let db = Pigeonhole::open(&path, Options::default()).unwrap();
    let store = pigeonhole_memory::Store::open_without_embeddings(&db, "agent").unwrap();
    assert_eq!(
        store.normalization(),
        pigeonhole_memory::Normalization::Nfkc
    );
    drop(store);
    db.close().unwrap();
    for flags in [
        &["--memory", "agent"][..],
        &["--memory", "agent", "--memory-normalization", "nfkc"][..],
    ] {
        let (status, err) = Server::start(&path, flags).finish();
        assert_eq!(status, 0, "{flags:?}: {err}");
    }
    let (status, err) = Server::spawn(
        &path,
        &["--memory", "agent", "--memory-normalization", "nfc"],
    )
    .finish();
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("is nfkc, fixed when it was created"), "{err}");
}

/// A reader process serves `memory_recall` only, over the writer's store.
#[test]
fn a_reader_recalls_the_writers_memories() {
    let dir = TempDir::new("mcp-memory-reader");
    let path = db_path(&dir);
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .create_if_missing(true),
    )
    .unwrap();
    // No store yet: a reader can't create one.
    let (status, err) = Server::spawn(&path, &["--read-only", "--memory", "agent"]).finish();
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("doesn't exist"), "{err}");
    let store =
        pigeonhole_memory::Store::create(&db, "agent", pigeonhole_memory::StoreOptions::new())
            .unwrap();
    let id = store
        .remember(
            "default",
            pigeonhole_memory::Memory::new("the build runs on Fridays"),
        )
        .unwrap()
        .id()
        .to_string();
    let mut s = Server::start(&path, &["--read-only", "--memory", "agent"]);
    let names = s.tool_names();
    assert!(names.contains(&"memory_recall".to_owned()), "{names:?}");
    assert!(!names.contains(&"memory_remember".to_owned()), "{names:?}");
    let hits = s.call("memory_recall", json!({"text": "build"}));
    assert_eq!(hits["hits"][0]["id"], json!(id), "{hits}");
    let e = s.fail("memory_remember", json!({"text": "x"}));
    assert_eq!(e["code"], "ReadOnly", "{e}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    drop(store);
    db.close().unwrap();
}

/// A stand-in for an OpenAI-compatible `/embeddings` endpoint: answers each request with a
/// 4-dimension embedding of its input (counts of letters), or HTTP 500 for an input that
/// starts with `FAIL:`, and records each request's `Authorization` header and body.
#[cfg(feature = "embed")]
struct Stub {
    url: String,
    seen: Seen,
}

/// The stub's record: each request's `Authorization` header and body.
#[cfg(feature = "embed")]
type Seen = std::sync::Arc<std::sync::Mutex<Vec<(Option<String>, Value)>>>;

#[cfg(feature = "embed")]
impl Stub {
    fn start() -> Self {
        use std::io::Read;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
        let seen: Seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = std::sync::Arc::clone(&seen);
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut conn) = conn else { return };
                let mut reader = BufReader::new(conn.try_clone().unwrap());
                let (mut auth, mut len) = (None, 0usize);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    let (k, v) = line.trim_end().split_once(": ").unwrap_or(("", ""));
                    match k.to_ascii_lowercase().as_str() {
                        "authorization" => auth = Some(v.to_owned()),
                        "content-length" => len = v.parse().unwrap(),
                        _ => {}
                    }
                }
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                let body: Value = serde_json::from_slice(&body).unwrap();
                let input = body["input"].as_str().unwrap_or("").to_owned();
                log.lock().unwrap().push((auth, body));
                let count = |c: char| input.chars().filter(|x| *x == c).count() as f32;
                let (status, reply) = if input.starts_with("FAIL:") {
                    (
                        "500 Internal Server Error",
                        json!({"error": {"message": "boom"}}),
                    )
                } else {
                    let e = [
                        input.len() as f32 + 1.0,
                        count('a') + 1.0,
                        count('e') + 1.0,
                        count(' ') + 1.0,
                    ];
                    (
                        "200 OK",
                        json!({"object": "list", "data": [{"object": "embedding", "index": 0, "embedding": e}], "model": "stub"}),
                    )
                };
                let reply = reply.to_string();
                let _ = write!(
                    conn,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        Self { url, seen }
    }

    fn requests(&self) -> Vec<(Option<String>, Value)> {
        self.seen.lock().unwrap().clone()
    }
}

/// `--embed-url` (part 3): the server embeds text through the endpoint, with the key from
/// the environment, pins a new store to the endpoint's model and dimension, and refuses a
/// client's vectors. The key never reaches stderr or a tool error.
#[cfg(feature = "embed")]
#[test]
fn embed_url_embeds_text_through_the_endpoint() {
    let dir = TempDir::new("mcp-memory-embed");
    let path = db_path(&dir);
    let stub = Stub::start();
    let key = ("PHDB_TEST_EMBED_KEY", "sekrit-key-123");
    let flags = [
        "--create",
        "--memory",
        "agent",
        "--embed-url",
        &stub.url,
        "--embed-model",
        "stub-e",
        "--embed-key-env",
        key.0,
    ];
    let (mut s, init) = Server::start_env(&path, &flags, &[key]);
    assert!(
        init["instructions"]
            .as_str()
            .unwrap()
            .contains("send text only"),
        "{init}"
    );
    let a = s.call(
        "memory_remember",
        json!({"text": "the cache sits in front of the database"}),
    );
    s.call("memory_remember", json!({"text": "zz"}));
    let hits = s.call(
        "memory_recall",
        json!({"text": "the cache sits in front of the database", "k": 1}),
    );
    assert_eq!(hits["hits"][0]["id"], a["id"], "{hits}");
    let e = s.fail(
        "memory_remember",
        json!({"text": "x", "embedding": [1.0, 1.0, 1.0, 1.0], "embedding_model": "stub-e"}),
    );
    assert!(
        e["message"].as_str().unwrap().contains("--embed-url"),
        "{e}"
    );
    // The endpoint's failure is the tool's, and nothing is written.
    let e = s.fail("memory_remember", json!({"text": "FAIL: zebra"}));
    let message = e["message"].as_str().unwrap();
    assert!(message.contains("HTTP 500"), "{e}");
    assert!(!message.contains(key.1), "{e}");
    let hits = s.call("memory_recall", json!({"text": "zebra"}));
    let texts: Vec<&str> = hits["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["text"].as_str().unwrap())
        .collect();
    assert_eq!(texts.len(), 2, "{hits}");
    assert!(texts.iter().all(|t| !t.contains("zebra")), "{hits}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    // `phdb mcp`'s startup line; `phdb serve` logs the store's creation its own way.
    if !transport::http() {
        assert!(
            err.contains("embedding model stub-e, 4 dimensions"),
            "{err}"
        );
    }
    assert!(!err.contains(key.1), "{err}");
    let seen = stub.requests();
    assert_eq!(seen[0].1["input"], "dimension probe", "{seen:?}");
    for (auth, body) in &seen {
        assert_eq!(auth.as_deref(), Some("Bearer sekrit-key-123"));
        assert_eq!(body["model"], "stub-e");
    }
    assert!(
        seen.iter()
            .any(|(_, b)| b["input"] == "the cache sits in front of the database"),
        "{seen:?}"
    );

    // An unset key variable, or another model than the store's, fails at startup.
    let mut unset = flags;
    unset[8] = "PHDB_TEST_EMBED_KEY_UNSET";
    let (status, err) = Server::spawn(&path, &unset).finish();
    assert_eq!(status, 2, "{err}");
    assert!(err.contains("PHDB_TEST_EMBED_KEY_UNSET"), "{err}");
    let mut other = flags;
    other[6] = "another-model";
    let (status, err) = Server::spawn_env(&path, &other, &[key]).finish();
    assert_eq!(status, 2, "{err}");
    assert!(
        err.contains("pinned to the embedding model \"stub-e\""),
        "{err}"
    );
}

/// Without the `embed` feature, `--embed-url` is refused as unsupported.
#[cfg(not(feature = "embed"))]
#[test]
fn embed_url_needs_the_embed_feature() {
    let dir = TempDir::new("mcp-memory-no-embed");
    let path = db_path(&dir);
    let (status, err) = Server::spawn(
        &path,
        &[
            "--create",
            "--memory",
            "agent",
            "--embed-url",
            "https://example.invalid/v1/embeddings",
            "--embed-model",
            "m",
        ],
    )
    .finish();
    assert_eq!(status, 6, "{err}");
    assert!(err.contains("embed"), "{err}");
}

/// `--embed-local`'s flags (#1345 PR 2): it's one way to embed, so it conflicts with
/// `--embed-url` and `--memory-model`, and its options need it. All usage errors, with or
/// without the feature.
#[test]
fn embed_local_conflicts_are_usage_errors() {
    let dir = TempDir::new("mcp-memory-local-flags");
    let path = db_path(&dir);
    for flags in [
        &[
            "--memory",
            "a",
            "--embed-local",
            "--embed-url",
            "http://127.0.0.1:9/v1/embeddings",
            "--embed-model",
            "m",
        ][..],
        &[
            "--memory",
            "a",
            "--embed-local",
            "--memory-model",
            "m",
            "--memory-dim",
            "4",
        ],
        &["--embed-local"],
        &["--memory", "a", "--embed-offline"],
        &["--memory", "a", "--embed-model-dir", "/nonexistent"],
    ] {
        let mut all = vec!["--create"];
        all.extend_from_slice(flags);
        let (status, err) = Server::spawn(&path, &all).finish();
        assert_eq!(status, 2, "{flags:?}: {err}");
    }
}

/// Without the `embed-local` feature, `--embed-local` is refused as unsupported.
#[cfg(not(feature = "embed-local"))]
#[test]
fn embed_local_needs_the_embed_local_feature() {
    let dir = TempDir::new("mcp-memory-no-local");
    let path = db_path(&dir);
    let (status, err) =
        Server::spawn(&path, &["--create", "--memory", "agent", "--embed-local"]).finish();
    assert_eq!(status, 6, "{err}");
    assert!(err.contains("embed-local"), "{err}");
}

/// `--embed-local` (#1345 PR 2): the model runs in the process, a new store is pinned to its
/// id and dimension with no probe, a memory is embedded as a passage and a recall's text as
/// a query, and a client's vectors are refused. It needs the model's files: set
/// `PIGEONHOLE_EMBED_MODEL_DIR` to a directory holding them, as `pigeonhole-embed`'s
/// reference test does (CI's `embed-reference` job).
#[cfg(feature = "embed-local")]
#[test]
fn embed_local_embeds_text_in_the_process() {
    let Some(model_dir) = std::env::var("PIGEONHOLE_EMBED_MODEL_DIR").ok() else {
        eprintln!("PIGEONHOLE_EMBED_MODEL_DIR is unset: skipping the local model");
        return;
    };
    let dir = TempDir::new("mcp-memory-local");
    let path = db_path(&dir);
    let flags = [
        "--create",
        "--memory",
        "agent",
        "--embed-local",
        "--embed-model-dir",
        &model_dir,
    ];
    let (mut s, init) = Server::start_env(&path, &flags, &[]);
    assert!(
        init["instructions"]
            .as_str()
            .unwrap()
            .contains("send text only"),
        "{init}"
    );
    let pg = s.call(
        "memory_remember",
        json!({"text": "The March planning meeting chose Postgres for the billing service."}),
    );
    s.call(
        "memory_remember",
        json!({"text": "Lunch on Friday was tacos from the place on Fifth Street."}),
    );
    // A query sharing no keyword with the memory: only the embedding finds it.
    let hits = s.call(
        "memory_recall",
        json!({"text": "which database did we pick?", "k": 1}),
    );
    assert_eq!(hits["hits"][0]["id"], pg["id"], "{hits}");
    let e = s.fail(
        "memory_remember",
        json!({"text": "x", "embedding": vec![0.0; 384], "embedding_model": "BAAI/bge-small-en-v1.5@5c38ec7c"}),
    );
    assert!(
        e["message"].as_str().unwrap().contains("--embed-local"),
        "{e}"
    );
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    assert!(
        err.contains("embedding model BAAI/bge-small-en-v1.5@5c38ec7c, 384 dimensions"),
        "{err}"
    );

    // Offline with an empty cache: the start fails, naming what to fetch.
    let empty = TempDir::new("mcp-memory-local-cache");
    let cache = empty.path().display().to_string();
    let (status, err) = Server::spawn_env(
        &path,
        &["--memory", "agent", "--embed-local", "--embed-offline"],
        &[("PIGEONHOLE_EMBED_CACHE", cache.as_str())],
    )
    .finish();
    assert_ne!(status, 0, "{err}");
    assert!(
        err.contains("--embed-local") && err.contains("huggingface.co"),
        "{err}"
    );
}

/// With `supersedes`, a store's near dedup doesn't stop the write (simp724's S1 on #1389): a
/// correction is near what it corrects, and `Skip` would otherwise store nothing and
/// supersede with the match.
#[test]
fn a_correction_is_stored_through_near_dedup() {
    let dir = TempDir::new("mcp-memory-near");
    let path = db_path(&dir);
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .create_if_missing(true),
    )
    .unwrap();
    let model = pigeonhole_memory::EmbeddingModel::new("m1", 4, pigeonhole::Element::F32).unwrap();
    let near = pigeonhole_memory::NearDedup::new(0.9).policy(pigeonhole_memory::DedupPolicy::Skip);
    drop(
        pigeonhole_memory::Store::create(
            &db,
            "agent",
            pigeonhole_memory::StoreOptions::new()
                .embedding(model)
                .near_dedup(near),
        )
        .unwrap(),
    );
    db.close().unwrap();
    let mut s = Server::start(&path, &["--memory", "agent"]);
    let five = s.call(
        "memory_remember",
        json!({"text": "the timeout is 5 s", "embedding": [1.0, 0.0, 0.0, 0.0], "embedding_model": "m1"}),
    );
    // Near dedup is on: a near memory without supersedes writes nothing.
    let near = s.call(
        "memory_remember",
        json!({"text": "the timeout is five seconds", "embedding": [0.99, 0.01, 0.0, 0.0], "embedding_model": "m1"}),
    );
    assert_eq!(
        (&near["outcome"], &near["id"]),
        (&json!("similar"), &five["id"]),
        "{near}"
    );
    // The correction is stored, and supersedes the old fact.
    let six = s.call(
        "memory_remember",
        json!({"text": "the timeout is 6 s", "embedding": [0.98, 0.02, 0.0, 0.0], "embedding_model": "m1", "supersedes": five["id"]}),
    );
    assert_eq!(
        (&six["outcome"], &six["superseded"]),
        (&json!("new"), &json!(true)),
        "{six}"
    );
    assert_ne!(six["id"], five["id"]);
    let hits = s.call(
        "memory_recall",
        json!({"text": "timeout", "include_superseded": true}),
    );
    let old = hits["hits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["id"] == five["id"])
        .unwrap_or_else(|| panic!("{hits}"));
    assert_eq!(old["superseded_by"], six["id"], "{hits}");
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
}

/// The same tests against `phdb serve`'s `/mcp` (D313 tickets 8c-3a and 8c-3b): the tools
/// reach the database and the memory store through the service layer there, and must answer
/// as `phdb mcp` does. (`--read-only` is a reader process, which a server has no
/// counterpart of: `/mcp`'s read tokens are tested in `serve`'s own tests.)
#[cfg(all(feature = "serve", unix))]
mod over_http {
    fn on_http(test: fn()) {
        super::transport::set_http();
        test();
    }

    #[test]
    fn a_client_handshakes_and_lists_the_tools() {
        on_http(super::a_client_handshakes_and_lists_the_tools);
    }

    #[test]
    fn protocol_mistakes_are_json_rpc_errors_and_tool_mistakes_are_results() {
        on_http(super::protocol_mistakes_are_json_rpc_errors_and_tool_mistakes_are_results);
    }

    #[test]
    fn create_table_reports_a_bad_family_before_a_bad_name() {
        on_http(super::create_table_reports_a_bad_family_before_a_bad_name);
    }

    #[test]
    fn a_batch_commits_every_operation_or_none() {
        on_http(super::a_batch_commits_every_operation_or_none);
    }

    #[test]
    fn long_results_are_cut_with_cursors_that_continue_them() {
        on_http(super::long_results_are_cut_with_cursors_that_continue_them);
    }

    #[test]
    fn traverse_follows_edges_hop_by_hop() {
        on_http(super::traverse_follows_edges_hop_by_hop);
    }

    #[test]
    fn traverse_pages_come_from_one_cached_result() {
        on_http(super::traverse_pages_come_from_one_cached_result);
    }

    #[test]
    fn vector_families_read_and_write_in_dump_shapes() {
        on_http(super::vector_families_read_and_write_in_dump_shapes);
    }

    #[test]
    fn nearest_ranks_vectors_under_the_selection() {
        on_http(super::nearest_ranks_vectors_under_the_selection);
    }

    #[test]
    fn feed_poll_continues_from_its_token_and_reports_expiry() {
        on_http(super::feed_poll_continues_from_its_token_and_reports_expiry);
    }

    #[test]
    fn a_new_vector_family_announces_nearest() {
        on_http(super::a_new_vector_family_announces_nearest);
    }
    #[test]
    fn memory_tools_remember_recall_supersede_and_forget() {
        on_http(super::memory_tools_remember_recall_supersede_and_forget);
    }

    #[test]
    fn a_pinned_store_refuses_another_models_embeddings() {
        on_http(super::a_pinned_store_refuses_another_models_embeddings);
    }

    #[test]
    fn a_new_stores_normalization_is_fixed_and_checked_at_startup() {
        on_http(super::a_new_stores_normalization_is_fixed_and_checked_at_startup);
    }

    #[test]
    fn a_correction_is_stored_through_near_dedup() {
        on_http(super::a_correction_is_stored_through_near_dedup);
    }

    #[cfg(feature = "embed")]
    #[test]
    fn embed_url_embeds_text_through_the_endpoint() {
        on_http(super::embed_url_embeds_text_through_the_endpoint);
    }
}
