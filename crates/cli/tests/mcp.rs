//! `phdb mcp` (#1249), driven as an MCP client drives it: the `phdb` binary as a child
//! process, one JSON-RPC message per line on its standard input and output.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use common::TempDir;
use pigeonhole::{Options, Pigeonhole};
use serde_json::{Value, json};

/// A running `phdb mcp`.
struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Server {
    /// Starts `phdb mcp <path> <flags>` and completes the handshake.
    fn start(path: &Path, flags: &[&str]) -> Self {
        let mut s = Self::spawn(path, flags);
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
        s
    }

    fn spawn(path: &Path, flags: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_phdb"))
            .arg("mcp")
            .arg(path)
            .args(flags)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    fn send(&mut self, msg: &Value) {
        self.send_line(&msg.to_string());
    }

    fn send_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
    }

    fn reply(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            self.stdout.read_line(&mut line).unwrap() > 0,
            "the server exited"
        );
        serde_json::from_str(&line).unwrap()
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

    /// Ends the input and waits for the server to exit, returning its status and stderr.
    fn finish(mut self) -> (i32, String) {
        drop(self.stdin.take());
        let out = self.child.wait_with_output().unwrap();
        (
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
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
    assert!(err.contains("as writer"), "{err}");
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

    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .create_if_missing(true),
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
        ["list_tables", "read_row", "scan", "traverse"]
    );
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
    // tools.
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let mut s = Server::start(&path, &[]);
    assert_eq!(s.tool_names().len(), 4);
    let (status, err) = s.finish();
    assert_eq!(status, 0, "{err}");
    db.close().unwrap();
}
