use super::*;

// ---- Memory (D313 ticket 8a) ----

/// A server with the memory store `m` (no embedding model: recall by keyword and recency).
fn memory_server(tokens: &str) -> Server {
    memory_server_with(tokens, limits())
}

fn memory_server_with(tokens: &str, l: Limits) -> Server {
    Server::start_custom(tokens, l, 1, |svc| {
        let (slot, _) = super::memslot::MemSlot::start(
            &svc.db,
            "m",
            None,
            None,
            Arc::new(|_| Ok(pigeonhole_memory::StoreOptions::new())),
            (1 << 20, 8 << 20),
            true,
        )
        .unwrap();
        svc.memory_opt(Some(slot), 0)
    })
}

fn remember(s: &Server, ns: &str, text: &str, token: Option<&str>) -> (u16, Json) {
    s.rpc(
        "Memory/Remember",
        &json!({"namespace": ns, "memory": {"text": text}}),
        token,
    )
}

#[test]
fn memories_are_remembered_recalled_marked_linked_superseded_and_forgotten() {
    let s = memory_server("");
    let (status, r) = remember(&s, "a", "the deploy key rotates on Fridays", None);
    assert_eq!(status, 200, "{r}");
    assert_eq!(r["outcome"].as_str(), Some("REMEMBER_OUTCOME_NEW"), "{r}");
    let first = r["id"].as_str().unwrap().to_owned();
    // The same text again is merged into it.
    let (_, r) = remember(&s, "a", "the deploy key rotates on Fridays", None);
    assert_eq!(
        r["outcome"].as_str(),
        Some("REMEMBER_OUTCOME_MERGED"),
        "{r}"
    );
    assert_eq!(r["id"].as_str(), Some(first.as_str()));
    let (_, r) = remember(&s, "a", "the deploy key now rotates on Mondays", None);
    assert_eq!(r["outcome"].as_str(), Some("REMEMBER_OUTCOME_NEW"), "{r}");
    let second = r["id"].as_str().unwrap().to_owned();
    // Recall by keyword, with signals.
    let (status, r) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["a"], "text": "deploy key", "k": 5, "with_signals": true}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    let hits = r["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2, "{r}");
    assert!(hits[0]["signals"].is_object(), "{r}");
    // Mark used, link, then supersede the old with the new: recall drops the old.
    let (_, r) = s.rpc("Memory/MarkUsed", &json!({"ids": [first]}), None);
    assert_eq!(r["marked"].as_str(), Some("1"), "{r}");
    let (status, e) = s.rpc(
        "Memory/Link",
        &json!({"from": second, "to": first, "link_type": {"builtin": "BUILTIN_LINK_SIMILAR_TO"}, "weight": 0.5}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    let (status, e) = s.rpc(
        "Memory/Supersede",
        &json!({"old": first, "new": second}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    let (_, r) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["a"], "text": "deploy key", "k": 5}),
        None,
    );
    let ids: Vec<&str> = r["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [second.as_str()], "{r}");
    // Forgetting the new one brings the old one back, as the store documents (its
    // `superseded_by` is cleared); forgetting that too leaves nothing.
    let (_, r) = s.rpc("Memory/Forget", &json!({"ids": [second]}), None);
    assert_eq!(r["forgotten"].as_str(), Some("1"), "{r}");
    let recalled = |s: &Server| {
        let (_, r) = s.rpc(
            "Memory/Recall",
            &json!({"namespaces": ["a"], "text": "deploy key", "k": 5}),
            None,
        );
        r["hits"]
            .as_array()
            .map(|h| {
                h.iter()
                    .map(|h| h["id"].as_str().unwrap().to_owned())
                    .collect()
            })
            .unwrap_or_else(Vec::new)
    };
    assert_eq!(recalled(&s), std::slice::from_ref(&first));
    let (_, r) = s.rpc("Memory/Forget", &json!({"ids": [first]}), None);
    assert_eq!(r["forgotten"].as_str(), Some("1"), "{r}");
    assert!(recalled(&s).is_empty());
    // Refused: an embedding for a store without a model, a bad id, a supersedes link, k
    // out of range, a budget past the server's cap.
    let v = f32_query(&[0.1, 0.2]);
    for (path, req) in [
        (
            "Memory/Remember",
            json!({"namespace": "a", "memory": {"text": "x", "embedding": v}}),
        ),
        ("Memory/MarkUsed", json!({"ids": [b64(b"not an id")]})),
        (
            "Memory/Link",
            json!({"from": first, "to": first, "link_type": {"builtin": "BUILTIN_LINK_SUPERSEDES"}}),
        ),
        (
            "Memory/Recall",
            json!({"namespaces": ["a"], "text": "x", "k": 0}),
        ),
        (
            "Memory/Recall",
            json!({"namespaces": ["a"], "text": "x", "k": 1, "byte_budget": (64u64 << 20).to_string()}),
        ),
        (
            "Memory/Recall",
            json!({"namespaces": [], "text": "x", "k": 1}),
        ),
    ] {
        let (_, e) = s.rpc(path, &req, None);
        assert_eq!(
            e["code"].as_str(),
            Some("InvalidArgument"),
            "{path} {req}: {e}"
        );
    }
    // Over gRPC too.
    let r = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::memory_client::MemoryClient::new(channel)
            .remember(proto::RememberRequest {
                namespace: "a".into(),
                memory: Some(proto::NewMemory {
                    text: "over gRPC".into(),
                    ..Default::default()
                }),
            })
            .await
            .unwrap()
            .into_inner()
    });
    assert_eq!(r.outcome, proto::RememberOutcome::New as i32);
}

#[test]
fn a_namespace_scoped_token_reaches_only_its_namespaces() {
    // ai1249's M4 on D313 §7: every Memory RPC checks the token's namespaces, against the
    // names a call gives and the namespace each id carries; a call outside them is refused
    // whole, never trimmed.
    let s = memory_server("admin all\nwrite ns=a scoped\nread ns=a reader\n");
    let (_, r) = remember(&s, "b", "in b", Some("all"));
    let in_b = r["id"].as_str().unwrap().to_owned();
    let (_, r) = remember(&s, "a", "in a", Some("all"));
    let in_a = r["id"].as_str().unwrap().to_owned();
    let denied = |path: &str, req: Json, token: &str| {
        let (status, e) = s.rpc(path, &req, Some(token));
        assert_eq!(
            (status, e["code"].as_str()),
            (403, Some("PermissionDenied")),
            "{path} {req}: {e}"
        );
    };
    denied(
        "Memory/Remember",
        json!({"namespace": "b", "memory": {"text": "x"}}),
        "scoped",
    );
    denied(
        "Memory/Remember",
        json!({"namespace": "a", "memory": {"text": "x", "links": [{"to": in_b}]}}),
        "scoped",
    );
    denied(
        "Memory/Recall",
        json!({"namespaces": ["a", "b"], "text": "in", "k": 5}),
        "scoped",
    );
    denied(
        "Memory/Recall",
        json!({"namespaces": ["b"], "text": "in", "k": 5}),
        "reader",
    );
    denied("Memory/MarkUsed", json!({"ids": [in_a, in_b]}), "scoped");
    denied(
        "Memory/Supersede",
        json!({"old": in_b, "new": in_a}),
        "scoped",
    );
    denied("Memory/Link", json!({"from": in_a, "to": in_b}), "scoped");
    denied("Memory/Forget", json!({"ids": [in_b]}), "scoped");
    denied("Memory/Watch", json!({"namespaces": ["b"]}), "scoped");
    // A watch of every namespace is everyone's: a scoped token names its own.
    denied("Memory/Watch", json!({}), "scoped");
    // A read token can't write, even in its namespace.
    denied(
        "Memory/Remember",
        json!({"namespace": "a", "memory": {"text": "x"}}),
        "reader",
    );
    // Inside the namespace, all fine; b's memory is untouched.
    assert_eq!(remember(&s, "a", "x", Some("scoped")).0, 200);
    let (status, r) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["a"], "text": "in", "k": 5}),
        Some("reader"),
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(
        s.rpc("Memory/Forget", &json!({"ids": [in_a]}), Some("scoped"))
            .0,
        200
    );
    let (_, r) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["b"], "text": "in", "k": 5}),
        Some("all"),
    );
    assert_eq!(r["hits"].as_array().map(Vec::len), Some(1), "{r}");
}

#[test]
fn a_memory_watch_streams_new_memories_and_resumes() {
    let s = memory_server("");
    let mut client = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::memory_client::MemoryClient::new(channel)
    });
    let mut stream = s
        .block_on(client.watch(proto::MemoryWatchRequest {
            namespaces: vec!["a".into()],
            resume: Default::default(),
        }))
        .unwrap()
        .into_inner();
    remember(&s, "b", "elsewhere", None);
    remember(&s, "a", "the first", None);
    let next = |stream: &mut tonic::Streaming<proto::MemoryEvent>| {
        s.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        })
    };
    // The memory in a comes as a memory; b's write, if delivered, only as progress.
    let token = loop {
        let e = next(&mut stream);
        match e.event {
            Some(proto::memory_event::Event::Memory(m)) => {
                assert_eq!((m.namespace.as_str(), m.text.as_str()), ("a", "the first"));
                break e.token;
            }
            Some(proto::memory_event::Event::Progress(_)) => {}
            None => panic!("an event without a body"),
        }
    };
    drop(stream);
    // A watch resumed from that token sees only what came after.
    remember(&s, "a", "the second", None);
    let mut resumed = s
        .block_on(client.watch(proto::MemoryWatchRequest {
            namespaces: vec!["a".into()],
            resume: token,
        }))
        .unwrap()
        .into_inner();
    loop {
        if let Some(proto::memory_event::Event::Memory(m)) = next(&mut resumed).event {
            assert_eq!(m.text, "the second");
            break;
        }
    }
}

#[test]
fn a_memory_watch_whose_client_leaves_gives_its_stream_slot_back() {
    // ai1249's R1 on #1617: a quiet store sends nothing, so a watch noticed its client gone
    // only at the next memory commit, holding its --max-streams slot until then. With one
    // slot, a second watch after the first client left must get it, with no commit between.
    let mut l = limits();
    l.max_streams = 1;
    let s = memory_server_with("", l);
    let watch = |s: &Server| {
        s.block_on(async {
            let channel = tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap();
            proto::memory_client::MemoryClient::new(channel)
                .watch(proto::MemoryWatchRequest {
                    namespaces: vec!["a".into()],
                    resume: Default::default(),
                })
                .await
        })
    };
    let first = watch(&s).unwrap();
    let busy = watch(&s).unwrap_err();
    assert_eq!(busy.code(), tonic::Code::Unavailable, "{busy:?}");
    drop(first);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match watch(&s) {
            Ok(_) => break,
            Err(e) if std::time::Instant::now() < deadline => {
                assert_eq!(e.code(), tonic::Code::Unavailable, "{e:?}");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => panic!("the first watch kept its slot after its client left: {e:?}"),
        }
    }
}

/// A stand-in for an `/embeddings` endpoint, by the input's prefix: `FAIL:` answers 500,
/// `BIG:` 413, `AUTH:` 401, `SHAPE:` a 200 that isn't an embedding, `DIM:` a 3-dimension
/// one, `SLOW:` after a second; anything else a 4-dimension embedding.
#[cfg(feature = "embed")]
fn embed_stub() -> String {
    use std::io::{BufRead, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(conn.try_clone().unwrap());
                let mut len = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                        len = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; len];
                if reader.read_exact(&mut body).is_err() {
                    return;
                }
                let body: Json = serde_json::from_slice(&body).unwrap();
                let input = body["input"].as_str().unwrap_or("").to_owned();
                let embedding = |dim: usize| {
                    let e: Vec<f32> = (0..dim).map(|i| (input.len() + i) as f32).collect();
                    json!({"data": [{"embedding": e}]})
                };
                let (status, reply) = match input.split_once(':').map(|(p, _)| p) {
                    Some("FAIL") => ("500 Internal Server Error", json!({"error": "boom"})),
                    Some("BIG") => ("413 Payload Too Large", json!({"error": "too long"})),
                    Some("AUTH") => ("401 Unauthorized", json!({"error": "bad key"})),
                    Some("SHAPE") => ("200 OK", json!({"data": []})),
                    Some("DIM") => ("200 OK", embedding(3)),
                    Some("SLOW") => {
                        std::thread::sleep(Duration::from_secs(1));
                        ("200 OK", embedding(4))
                    }
                    _ => ("200 OK", embedding(4)),
                };
                let reply = reply.to_string();
                let _ = write!(
                    conn,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            });
        }
    });
    url
}

/// A memory server whose store is pinned to the stub's 4-dimension model and embeds through
/// `url`, each call bounded by 300 ms.
#[cfg(feature = "embed")]
fn embedding_memory_server(url: &str) -> Server {
    let url = url.to_owned();
    Server::start_custom("", limits(), 1, move |svc| {
        let endpoint =
            crate::mcp::Endpoint::new(&url, "stub", None, Duration::from_millis(300)).unwrap();
        let (slot, _) = super::memslot::MemSlot::start(
            &svc.db,
            "m",
            Some(crate::memcore::Embedder::Remote(endpoint)),
            None,
            Arc::new(|_| {
                let model =
                    pigeonhole_memory::EmbeddingModel::new("stub", 4, pigeonhole::Element::F32)
                        .unwrap();
                Ok(pigeonhole_memory::StoreOptions::new().embedding(model))
            }),
            (1 << 20, 8 << 20),
            true,
        )
        .unwrap();
        svc.memory_opt(Some(slot), 0)
    })
}

#[cfg(feature = "embed")]
#[test]
fn memory_through_an_embedding_endpoint_answers_its_failures_by_kind() {
    // ai1249's N5 and R2 on #1617: the server embeds the text itself, refuses an embedding
    // sent with it, and answers each kind of endpoint failure with its own code: unreachable
    // or failing is Unavailable (retry-safe), its timeout DeadlineExceeded, a refusal of the
    // text InvalidArgument, and anything else (its key, its reply) Internal.
    let s = embedding_memory_server(&embed_stub());
    let (status, r) = remember(&s, "a", "the deploy key rotates", None);
    assert_eq!(status, 200, "{r}");
    let (status, r) = s.rpc(
        "Memory/Recall",
        &json!({"namespaces": ["a"], "text": "deploy key", "k": 5}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(r["hits"].as_array().map(Vec::len), Some(1), "{r}");
    let given = json!({"namespace": "a", "memory": {"text": "x", "embedding": f32_query(&[0.1, 0.2, 0.3, 0.4])}});
    let (status, e) = s.rpc("Memory/Remember", &given, None);
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );

    for (text, want) in [
        ("FAIL: x", (503, "Unavailable")),
        ("BIG: x", (400, "InvalidArgument")),
        ("AUTH: x", (500, "Internal")),
        ("SHAPE: x", (500, "Internal")),
        ("DIM: x", (500, "Internal")),
        ("SLOW: x", (504, "DeadlineExceeded")),
    ] {
        let (status, e) = remember(&s, "a", text, None);
        assert_eq!(
            (status, e["code"].as_str()),
            (want.0, Some(want.1)),
            "{text}: {e}"
        );
        if want.1 == "Unavailable" {
            assert_eq!(e["retry_from"], "RETRY_FROM_REQUEST", "{e}");
        }
        // A recall's query text goes the same way.
        let (status, e) = s.rpc(
            "Memory/Recall",
            &json!({"namespaces": ["a"], "text": text, "k": 5}),
            None,
        );
        assert_eq!(
            (status, e["code"].as_str()),
            (want.0, Some(want.1)),
            "recall {text}: {e}"
        );
    }

    // An endpoint that refuses the connection, and one whose connect never completes (a
    // non-routable address: the SYN goes nowhere, or the route is refused at once): both
    // Unavailable, the second at the connect bound (150 ms here), never DeadlineExceeded.
    // Windows retries a SYN to a closed port for about 2 s, so even the first took the
    // second's path there (#1617).
    let closed = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}/v1/embeddings", l.local_addr().unwrap())
    };
    for url in [closed.as_str(), "http://10.255.255.1:9/v1/embeddings"] {
        let s = embedding_memory_server(url);
        let (status, e) = remember(&s, "a", "anything", None);
        assert_eq!(
            (status, e["code"].as_str()),
            (503, Some("Unavailable")),
            "{url}: {e}"
        );
    }
}

#[test]
fn memory_calls_without_a_store_are_unsupported() {
    let s = Server::start("", limits());
    let (_, e) = remember(&s, "a", "x", None);
    assert_eq!(e["code"].as_str(), Some("Unsupported"), "{e}");
}
