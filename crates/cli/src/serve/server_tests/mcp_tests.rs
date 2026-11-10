use super::*;

// ---- Remote MCP at /mcp (D313 §7, ticket 8c-3a) ----
//
// The tools themselves are `tests/mcp.rs`'s, run against `/mcp` too (its `over_http`). These
// are the server's own rules: sessions and their tokens, the protocol version header, the
// scopes, the service's table rules, and a session's request limit.

/// One `POST /mcp`'s answer.
struct Answer {
    status: u16,
    session: Option<String>,
    sse: bool,
    /// The JSON-RPC messages: the body, or each SSE event's data.
    messages: Vec<Json>,
}

impl Answer {
    /// The reply (the message with an `id`).
    fn reply(&self) -> &Json {
        self.messages
            .iter()
            .find(|m| m.get("id").is_some())
            .unwrap_or_else(|| panic!("no reply: {:?}", self.messages))
    }
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        // A request that hangs fails the test rather than hanging it.
        .timeout_global(Some(Duration::from_secs(20)))
        .build()
        .into()
}

/// `POST /mcp` with `body`, as `token`, on `session`, naming `version`.
fn mcp(
    s: &Server,
    token: Option<&str>,
    session: Option<&str>,
    version: Option<&str>,
    body: &Json,
) -> Answer {
    let mut req = agent()
        .post(format!("{}/mcp", s.url))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    if let Some(id) = session {
        req = req.header("mcp-session-id", id);
    }
    if let Some(v) = version {
        req = req.header("mcp-protocol-version", v);
    }
    let mut resp = req
        .send(body.to_string())
        .unwrap_or_else(|e| panic!("POST /mcp: {e:?}"));
    let status = resp.status().as_u16();
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_owned())
    };
    let session = header("mcp-session-id");
    let sse = header("content-type").is_some_and(|c| c.starts_with("text/event-stream"));
    let text = resp.body_mut().read_to_string().unwrap();
    let messages = if sse {
        text.lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .map(|d| serde_json::from_str(d).unwrap())
            .collect()
    } else if text.is_empty() {
        Vec::new()
    } else {
        vec![serde_json::from_str(&text).unwrap()]
    };
    Answer {
        status,
        session,
        sse,
        messages,
    }
}

/// A new session for `token`, initialized: its id.
fn session(s: &Server, token: Option<&str>) -> String {
    let init = mcp(
        s,
        token,
        None,
        None,
        &json!({"jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18"}}),
    );
    assert_eq!(init.status, 200, "{:?}", init.messages);
    assert_eq!(init.reply()["result"]["protocolVersion"], "2025-06-18");
    let id = init.session.expect("initialize names the session");
    let note = mcp(
        s,
        token,
        Some(&id),
        Some("2025-06-18"),
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    );
    assert_eq!(note.status, 202);
    id
}

fn request(method: &str, params: Json) -> Json {
    json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
}

/// `tools/call`: the result's structured content, and whether it's an error.
fn call(s: &Server, token: Option<&str>, id: &str, tool: &str, args: Json) -> (Json, bool) {
    let a = mcp(
        s,
        token,
        Some(id),
        Some("2025-06-18"),
        &request("tools/call", json!({"name": tool, "arguments": args})),
    );
    assert_eq!(a.status, 200, "{:?}", a.messages);
    let result = &a.reply()["result"];
    (
        result["structuredContent"].clone(),
        result["isError"].as_bool().unwrap(),
    )
}

fn tool_names(s: &Server, token: Option<&str>, id: &str) -> Vec<String> {
    let a = mcp(
        s,
        token,
        Some(id),
        Some("2025-06-18"),
        &request("tools/list", json!({})),
    );
    a.reply()["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_owned())
        .collect()
}

fn delete(s: &Server, token: Option<&str>, id: &str) -> u16 {
    let mut req = agent()
        .delete(format!("{}/mcp", s.url))
        .header("mcp-session-id", id);
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    req.call().unwrap().status().as_u16()
}

#[test]
fn an_mcp_session_belongs_to_its_token_and_ends_with_delete() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let id = session(&s, Some("wr1te"));
    let metrics = s.shared.router.service.metrics_text();
    assert!(
        metrics.contains("pigeonhole_serve_sessions{kind=\"mcp\"} 1"),
        "{metrics}"
    );
    // Another token's request on it, or its end: the session isn't there for it (M1).
    let ping = request("ping", json!({}));
    let other = mcp(&s, Some("r3ad"), Some(&id), None, &ping);
    assert_eq!(other.status, 404);
    assert_eq!(delete(&s, Some("r3ad"), &id), 404);
    assert_eq!(mcp(&s, Some("wr1te"), Some(&id), None, &ping).status, 200);
    // No token at all is refused before any session is looked at.
    assert_eq!(mcp(&s, None, Some(&id), None, &ping).status, 401);
    // Ended: the client gets 404, which tells it to start a new session.
    assert_eq!(delete(&s, Some("wr1te"), &id), 204);
    assert_eq!(mcp(&s, Some("wr1te"), Some(&id), None, &ping).status, 404);
    let metrics = s.shared.router.service.metrics_text();
    assert!(
        metrics.contains("pigeonhole_serve_sessions{kind=\"mcp\"} 0"),
        "{metrics}"
    );
}

#[test]
fn only_initialize_starts_a_session_and_get_is_not_allowed() {
    let s = Server::start("", limits());
    let list = mcp(&s, None, None, None, &request("tools/list", json!({})));
    assert_eq!(list.status, 400);
    assert!(list.session.is_none());
    let resp = agent().get(format!("{}/mcp", s.url)).call().unwrap();
    assert_eq!(resp.status().as_u16(), 405);
    assert_eq!(resp.headers().get("allow").unwrap(), "POST, DELETE");
    // `/mcp` offers 2025-03-26 and later: an older client is answered with the newest
    // (ai1249's M3), and the stdio server's 2024-11-05 isn't among them.
    let old = mcp(
        &s,
        None,
        None,
        None,
        &json!({"jsonrpc": "2.0", "id": 0, "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"}}),
    );
    assert_eq!(old.reply()["result"]["protocolVersion"], "2025-11-25");
}

#[test]
fn a_request_naming_another_protocol_version_is_refused() {
    let s = Server::start("", limits());
    let id = session(&s, None);
    let ping = request("ping", json!({}));
    let other = mcp(&s, None, Some(&id), Some("2025-03-26"), &ping);
    assert_eq!(other.status, 400);
    // The session's version, or none (the spec's fallback), is answered.
    assert_eq!(
        mcp(&s, None, Some(&id), Some("2025-06-18"), &ping).status,
        200
    );
    assert_eq!(mcp(&s, None, Some(&id), None, &ping).status, 200);
}

#[test]
fn a_read_token_is_offered_the_read_tools_and_refused_writes() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let reader = session(&s, Some("r3ad"));
    let names = tool_names(&s, Some("r3ad"), &reader);
    assert!(names.contains(&"read_row".to_owned()), "{names:?}");
    assert!(!names.contains(&"write".to_owned()), "{names:?}");
    assert!(!names.contains(&"create_table".to_owned()), "{names:?}");
    // A write tool called anyway passes `admit` as an RPC does: refused, as the tool's result.
    let (e, err) = call(
        &s,
        Some("r3ad"),
        &reader,
        "write",
        json!({"ops": [{"table": "t", "row": "r", "family": "f", "qualifier": "q",
                        "op": "put", "value": "v"}]}),
    );
    assert!(err, "{e}");
    assert_eq!(e["error"]["code"], "PermissionDenied", "{e}");
    // The write token writes, and the reader reads it.
    let writer = session(&s, Some("wr1te"));
    assert!(tool_names(&s, Some("wr1te"), &writer).contains(&"write".to_owned()));
    let (out, err) = call(
        &s,
        Some("wr1te"),
        &writer,
        "write",
        json!({"ops": [{"table": "t", "row": "r", "family": "f", "qualifier": "q",
                        "op": "put", "value": "v"}]}),
    );
    assert!(!err, "{out}");
    let (row, err) = call(
        &s,
        Some("r3ad"),
        &reader,
        "read_row",
        json!({"table": "t", "row": "r"}),
    );
    assert!(!err, "{row}");
    assert!(row.to_string().contains("\"v\""), "{row}");
}

#[test]
fn the_tools_keep_to_the_services_table_rules() {
    let s = Server::start("", limits());
    let id = session(&s, None);
    // The cluster's reserved prefix (ICR 0117), as `Schema.CreateTable` refuses it.
    let (e, err) = call(
        &s,
        None,
        &id,
        "create_table",
        json!({"table": "__phdb_mine", "families": ["f"]}),
    );
    assert!(err, "{e}");
    assert!(
        e["error"]["message"]
            .as_str()
            .unwrap()
            .contains("prefix is reserved"),
        "{e}"
    );
    let (e, err) = call(
        &s,
        None,
        &id,
        "read_row",
        json!({"table": "__phdb_cluster", "row": "r"}),
    );
    assert!(err, "{e}");
    assert!(
        e["error"]["message"]
            .as_str()
            .unwrap()
            .contains("prefix is reserved"),
        "{e}"
    );
    // A table the tool creates is the service's: the RPCs see it at once.
    let (out, err) = call(
        &s,
        None,
        &id,
        "create_table",
        json!({"table": "made", "families": ["f"]}),
    );
    assert!(!err, "{out}");
    let (status, tables) = s.rpc("Schema/ListTables", &json!({}), None);
    assert_eq!(status, 200, "{tables}");
    assert_eq!(tables["tables"], json!(["made", "t"]));
}

#[test]
fn another_sessions_new_vector_family_announces_nearest() {
    let s = Server::start("", limits());
    let a = session(&s, None);
    let b = session(&s, None);
    assert!(!tool_names(&s, None, &a).contains(&"nearest".to_owned()));
    let (out, err) = call(
        &s,
        None,
        &b,
        "create_table",
        json!({"table": "docs", "families": [
            {"name": "emb", "vector": {"dim": 2, "element": "f32", "metric": "l2"}}
        ]}),
    );
    assert!(!err, "{out}");
    // A's next request carries the notification after its reply, as SSE (M2).
    let ping = mcp(&s, None, Some(&a), None, &request("ping", json!({})));
    assert!(ping.sse, "{:?}", ping.messages);
    assert_eq!(
        ping.messages,
        [
            json!({"jsonrpc": "2.0", "id": 1, "result": {}}),
            json!({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"}),
        ]
    );
    // Once per change: the next is plain JSON.
    let again = mcp(&s, None, Some(&a), None, &request("ping", json!({})));
    assert!(!again.sse);
    assert!(tool_names(&s, None, &a).contains(&"nearest".to_owned()));
}

#[test]
fn a_session_past_its_request_limit_gets_a_retryable_refusal() {
    let mut l = limits();
    l.mcp_session_requests = 1;
    let s = Server::start("", l);
    let id = session(&s, None);
    // A poll waiting for a commit holds the session's one place.
    let url = s.url.clone();
    let held = id.clone();
    let poll = std::thread::spawn(move || {
        let mut resp = agent()
            .post(format!("{url}/mcp"))
            .header("content-type", "application/json")
            .header("mcp-session-id", held.as_str())
            .send(
                request(
                    "tools/call",
                    json!({"name": "feed_poll", "arguments": {"tables": ["t"], "wait_ms": 4000}}),
                )
                .to_string(),
            )
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        resp.body_mut().read_to_string().unwrap()
    });
    let busy = s
        .shared
        .router
        .service
        .sessions
        .stats(std::time::Instant::now());
    assert_eq!(busy.mcp, 1);
    std::thread::sleep(Duration::from_millis(500));
    let mut resp = agent()
        .post(format!("{}/mcp", s.url))
        .header("content-type", "application/json")
        .header("mcp-session-id", id.as_str())
        .send(request("ping", json!({})).to_string())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 503);
    assert!(resp.headers().get("retry-after").is_some(), "{resp:?}");
    let refused: Json = resp.body_mut().read_json().unwrap();
    assert_eq!(refused["code"], "Overloaded");
    let polled = poll.join().unwrap();
    assert!(polled.contains("\"isError\":false"), "{polled}");
    // The place is back.
    assert_eq!(
        mcp(&s, None, Some(&id), None, &request("ping", json!({}))).status,
        200
    );
}

#[test]
fn an_initialize_without_an_answer_leaves_no_session() {
    let s = Server::start("", limits());
    // As a notification (no id), nothing answers it: the session it opened goes again.
    let a = mcp(
        &s,
        None,
        None,
        None,
        &json!({"jsonrpc": "2.0", "method": "initialize", "params": {}}),
    );
    assert_eq!(a.status, 202);
    assert!(a.session.is_none());
    let stats = s
        .shared
        .router
        .service
        .sessions
        .stats(std::time::Instant::now());
    assert_eq!(stats.mcp, 0);
}

#[test]
fn a_tool_opens_and_creates_tables_on_the_requests_own_pool_thread() {
    // ai1249's B1 on #1666: a `/mcp` request holds a blocking-pool thread for its message,
    // so a table it opens or creates must not wait for a second one. With a pool of one,
    // that would wait forever.
    blocking_threads(1);
    let s = Server::start("", limits());
    let id = session(&s, None);
    let (out, err) = call(
        &s,
        None,
        &id,
        "create_table",
        json!({"table": "made", "families": ["f"]}),
    );
    assert!(!err, "{out}");
    // A drop empties the table cache: the next use of each name opens it again.
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "made"}), None);
    assert_eq!(status, 200, "{e}");
    let (out, err) = call(&s, None, &id, "read_row", json!({"table": "t", "row": "r"}));
    assert!(!err, "{out}");
    let (out, err) = call(&s, None, &id, "list_tables", json!({}));
    assert!(!err, "{out}");
}

#[test]
fn sessions_opening_tables_at_once_never_wait_on_each_other() {
    // As many first reads of uncached tables at once as the pool has threads (2): each
    // request holds one thread, and opens its table on it.
    let s = Server::start("", limits());
    for name in ["a", "b", "c", "d"] {
        let (status, e) = s.rpc(
            "Schema/CreateTable",
            &json!({"name": name, "families": [{"name": "f"}]}),
            None,
        );
        assert_eq!(status, 200, "{e}");
    }
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "d"}), None);
    assert_eq!(status, 200, "{e}");
    let ids: Vec<String> = (0..3).map(|_| session(&s, None)).collect();
    let url = s.url.clone();
    let readers: Vec<_> = ids
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(id, table)| {
            let url = url.clone();
            std::thread::spawn(move || {
                let mut resp = agent()
                    .post(format!("{url}/mcp"))
                    .header("content-type", "application/json")
                    .header("mcp-session-id", id.as_str())
                    .send(
                        request(
                            "tools/call",
                            json!({"name": "read_row", "arguments": {"table": table, "row": "r"}}),
                        )
                        .to_string(),
                    )
                    .unwrap();
                assert_eq!(resp.status().as_u16(), 200);
                resp.body_mut().read_to_string().unwrap()
            })
        })
        .collect();
    for r in readers {
        let body = r.join().unwrap();
        assert!(body.contains("\"isError\":false"), "{body}");
    }
}

#[test]
fn a_tool_create_racing_a_drop_takes_the_pool_and_schema_in_one_order() {
    // ai1249's B2 on #1666: a `/mcp` create holds its request's pool thread and then takes
    // the schema lock; an RPC create or drop must take its pool place before that lock too,
    // or with a pool of one each waits on the other for good.
    blocking_threads(1);
    let s = Server::start("", limits());
    let id = session(&s, None);
    for i in 0..20 {
        let doomed = format!("d{i}");
        let (status, e) = s.rpc(
            "Schema/CreateTable",
            &json!({"name": doomed, "families": [{"name": "f"}]}),
            None,
        );
        assert_eq!(status, 200, "{e}");
        let url = s.url.clone();
        let dropper = std::thread::spawn(move || {
            agent()
                .post(format!("{url}/v1/Schema/DropTable"))
                .header("content-type", "application/json")
                .send(json!({"name": doomed}).to_string())
                .unwrap()
                .status()
                .as_u16()
        });
        let (out, err) = call(
            &s,
            None,
            &id,
            "create_table",
            json!({"table": format!("c{i}"), "families": ["f"]}),
        );
        assert!(!err, "{out}");
        assert_eq!(dropper.join().unwrap(), 200);
    }
}
