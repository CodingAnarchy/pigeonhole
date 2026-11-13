use super::*;

// ---- Graph.Traverse and Vectors.Nearest (D313 ticket 7) ----

/// `g`, an edge family `e`: a -> b, a -> c, b -> d, and c -> x:skip (an edge `qualifier_prefix`
/// b"x" alone keeps).
fn graph(s: &Server) -> pigeonhole::Table {
    let db = &s.shared.router.service.part().unwrap().db;
    let g = db
        .table("g")
        .unwrap()
        .family("e", Family::default())
        .create()
        .unwrap();
    for (from, to) in [
        (&b"a"[..], &b"b"[..]),
        (b"a", b"c"),
        (b"b", b"d"),
        (b"c", b"x:skip"),
    ] {
        g.mutate(from).put("e", to, b"").commit().unwrap();
    }
    g
}

fn hops_of(r: &Json) -> Vec<Vec<String>> {
    r["hops"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            h["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| n.as_str().unwrap().to_owned())
                .collect()
        })
        .collect()
}

#[test]
fn traverse_walks_an_edge_family_hop_by_hop() {
    let s = Server::start("", limits());
    let g = graph(&s);
    let (status, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 2, "parents": true}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(
        hops_of(&r),
        [vec![b64(b"b"), b64(b"c")], vec![b64(b"d"), b64(b"x:skip")]]
    );
    // Each node's source, as an index into the previous hop (the seeds for hop 1).
    assert_eq!(r["hops"][0]["parents"], json!([0, 0]));
    assert_eq!(r["hops"][1]["parents"], json!([0, 1]));
    // A uint64, so a string in proto3's JSON mapping.
    assert_eq!(r["stats"]["edges"], json!("4"), "{r}");
    // The edge selection: only qualifiers under `x`.
    let (_, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"c")],
                "qualifier_prefix": b64(b"x")}),
        None,
    );
    assert_eq!(hops_of(&r), [vec![b64(b"x:skip")]]);
    // At a pinned snapshot: an edge added after it isn't followed.
    let (_, opened) = s.rpc("Snapshots/Open", &json!({}), None);
    let id = opened["snapshot_id"].as_str().unwrap().to_owned();
    g.mutate(b"d").put("e", b"later", b"").commit().unwrap();
    let walk = |snapshot: Option<&str>| {
        let mut req = json!({"table": "g", "family": "e", "seeds": [b64(b"b")], "hops": 2});
        if let Some(id) = snapshot {
            req["snapshot_id"] = json!(id);
        }
        hops_of(&s.rpc("Graph/Traverse", &req, None).1)
    };
    assert_eq!(walk(Some(&id)), [vec![b64(b"d")]]);
    assert_eq!(walk(None), [vec![b64(b"d")], vec![b64(b"later")]]);
    // A reply is byte-budgeted (simp724's L1 on #1565): a node counts its key and 16 bytes,
    // so 20 bytes keep one of hop 1's two nodes, and the traversal ends there.
    let (_, r) = s.rpc(
        "Graph/Traverse",
        &json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 2,
                "page_bytes": 20}),
        None,
    );
    assert_eq!(hops_of(&r).len(), 1, "{r}");
    assert_eq!(hops_of(&r)[0].len(), 1, "{r}");
    assert_eq!(r["stats"]["cut_by_budget"], json!(true), "{r}");
    // Over gRPC.
    let got = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::graph_client::GraphClient::new(channel)
            .traverse(proto::TraverseRequest {
                table: "g".into(),
                family: "e".into(),
                seeds: vec![b"a".to_vec().into()],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
    });
    assert_eq!(got.hops.len(), 1);
    assert_eq!(got.hops[0].nodes, [b"b".to_vec(), b"c".to_vec()]);
}

#[test]
fn traverse_refuses_what_it_cannot_walk() {
    let s = Server::start("", limits());
    graph(&s);
    for (req, code) in [
        (
            json!({"table": "g", "family": "e", "seeds": []}),
            "InvalidArgument",
        ),
        (
            json!({"table": "g", "family": "e", "seeds": [b64(b"a")], "hops": 65}),
            "InvalidArgument",
        ),
        (
            json!({"table": "nope", "family": "e", "seeds": [b64(b"a")]}),
            "TableNotFound",
        ),
        (
            json!({"table": "g", "family": "nope", "seeds": [b64(b"a")]}),
            "FamilyNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Graph/Traverse", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
}

/// `v`, an `f32` L2 vector family `emb` of dim 2: rows a [0, 1], b [0, 2] and c [0, 3].
fn vectors(s: &Server) -> pigeonhole::Table {
    let db = &s.shared.router.service.part().unwrap().db;
    let spec =
        pigeonhole::VectorSpec::new(2, pigeonhole::Element::F32, pigeonhole::Metric::L2).unwrap();
    let v = db
        .table("v")
        .unwrap()
        .family("emb", Family::vector(spec))
        .family("f", Family::default())
        .create()
        .unwrap();
    for (row, y) in [(&b"a"[..], 1.0f32), (b"b", 2.0), (b"c", 3.0)] {
        v.mutate(row)
            .put_vector_f32("emb", b"", &[0.0, y])
            .commit()
            .unwrap();
    }
    v
}

fn rows_of(r: &Json) -> Vec<String> {
    r["neighbours"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["row"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn nearest_ranks_a_ranges_or_a_key_lists_vectors() {
    let s = Server::start("", limits());
    vectors(&s);
    let q = f32_query(&[0.0, 0.0]);
    let (status, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 2}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(rows_of(&r), [b64(b"a"), b64(b"b")]);
    assert!(
        r["neighbours"][0]["distance"].as_f64().unwrap()
            < r["neighbours"][1]["distance"].as_f64().unwrap()
    );
    // A key list, and a range.
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 5,
                "keys": {"keys": [b64(b"c"), b64(b"b")]}}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"b"), b64(b"c")]);
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 5,
                "range": {"prefix": b64(b"c")}}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"c")]);
    // D308's budget: each neighbour is its 1-byte row, an empty qualifier and 16 bytes.
    let (_, r) = s.rpc(
        "Vectors/Nearest",
        &json!({"table": "v", "family": "emb", "query": q, "k": 3, "page_bytes": 40}),
        None,
    );
    assert_eq!(rows_of(&r), [b64(b"a"), b64(b"b")]);
    assert_eq!(r["cut_by_budget"], json!(true), "{r}");
}

#[test]
fn nearest_refuses_a_query_it_cannot_run() {
    let s = Server::start("", limits());
    vectors(&s);
    let q = f32_query(&[0.0, 0.0]);
    let half = json!({"element": "ELEMENT_F16", "dim": 2, "data": b64(&[0; 4])});
    let i8q = json!({"element": "ELEMENT_I8", "dim": 2, "data": b64(&[0; 2])});
    for (req, code) in [
        (
            json!({"table": "v", "family": "emb", "query": q, "k": 0}),
            "InvalidArgument",
        ),
        // A ranking keeps `k` candidates before the reply's budget cuts it: k is capped.
        (
            json!({"table": "v", "family": "emb", "query": q, "k": 10_001}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "query": half, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "emb", "query": i8q, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "v", "family": "f", "query": q, "k": 1}),
            "InvalidArgument",
        ),
        (
            json!({"table": "nope", "family": "emb", "query": q, "k": 1}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Vectors/Nearest", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
}

// ---- Vectors' IVF index (D313 ticket 7b1, ICR 0100) ----

#[test]
fn an_ivf_index_is_created_written_searched_and_verified() {
    let s = Server::start("", limits());
    let (status, e) = s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!(status, 200, "{e}");
    // A second create is TableExists, unless `if_missing`.
    let (_, e) = s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!(e["code"].as_str(), Some("TableExists"), "{e}");
    let mut again = index_spec("REBALANCE_INLINE");
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Vectors/CreateIndex", &again, None).0, 200);
    for (id, y) in [("a", 1.0f32), ("b", 2.0), ("c", 3.0), ("x:d", 4.0)] {
        let (status, e) = s.rpc(
            "Vectors/Insert",
            &json!({"index": "ix", "id": b64(id.as_bytes()), "vector": f32_query(&[0.0, y])}),
            None,
        );
        assert_eq!(status, 200, "{id}: {e}");
    }
    let q = f32_query(&[0.0, 0.0]);
    let (status, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 2, "stats": true}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(hit_ids(&r), [b64(b"a"), b64(b"b")]);
    assert!(r["stats"]["lists"].as_u64().unwrap() >= 1, "{r}");
    // Only ids under a prefix.
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 3, "id_prefix": b64(b"x:")}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"x:d")]);
    assert!(
        r.get("stats").is_none_or(Json::is_null),
        "stats only when asked: {r}"
    );
    // A delete, then the index's own check.
    assert_eq!(
        s.rpc(
            "Vectors/Delete",
            &json!({"index": "ix", "id": b64(b"b")}),
            None
        )
        .0,
        200
    );
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 2}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"a"), b64(b"c")]);
    let (status, v) = s.rpc("Vectors/Verify", &json!({"index": "ix"}), None);
    assert_eq!((status, &v["vectors"]), (200, &json!("3")), "{v}");
    // Refused: another element type, k out of range, an index that doesn't exist.
    let i8v = json!({"element": "ELEMENT_I8", "dim": 2, "data": b64(&[0; 2])});
    for (path, req, code) in [
        (
            "Vectors/Insert",
            json!({"index": "ix", "id": b64(b"e"), "vector": i8v}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "ix", "query": q, "k": 0}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "ix", "query": q, "k": 10_001}),
            "InvalidArgument",
        ),
        (
            "Vectors/Search",
            json!({"index": "nope", "query": q, "k": 1}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc(path, &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{path} {req}: {status} {e}");
    }
    // Over gRPC.
    let verified = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::vectors_client::VectorsClient::new(channel)
            .verify(proto::VectorVerifyRequest { index: "ix".into() })
            .await
            .unwrap()
            .into_inner()
    });
    assert_eq!(verified.vectors, 3);
}

#[test]
fn a_background_index_rebalances_on_a_server_thread_that_close_ends() {
    // The coordinator on 7b1: the server runs a background index's rebalancer itself, so
    // its shutdown can wait for the thread before it closes the database.
    let s = Server::start("", limits());
    let indexes = &s.shared.router.service.part().unwrap().indexes;
    assert_eq!(
        s.rpc(
            "Vectors/CreateIndex",
            &index_spec("REBALANCE_BACKGROUND"),
            None
        )
        .0,
        200
    );
    assert_eq!(
        indexes.rebalancing(),
        1,
        "one thread for the background index"
    );
    for i in 0..50u32 {
        let (status, e) = s.rpc(
            "Vectors/Insert",
            &json!({"index": "ix", "id": b64(format!("v{i:03}").as_bytes()),
                    "vector": f32_query(&[i as f32, 1.0])}),
            None,
        );
        assert_eq!(status, 200, "{e}");
    }
    let started = std::time::Instant::now();
    indexes.close();
    assert_eq!(indexes.rebalancing(), 0, "close waited for the thread");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the thread saw its index close within one idle wait: {:?}",
        started.elapsed()
    );
    // After close, nothing opens again (ai1249's M2 on #1573): a call still queued on the
    // blocking pool at shutdown, or a request, is Closed, and starts no rebalancer.
    let late = indexes.get("ix");
    assert_eq!(late.err().map(|e| e.code), Some("Closed".to_owned()));
    let (_, e) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": f32_query(&[0.0, 1.0]), "k": 1}),
        None,
    );
    assert_eq!(e["code"].as_str(), Some("Closed"), "{e}");
    assert_eq!(indexes.rebalancing(), 0, "no thread after close");
}

#[test]
fn a_write_list_that_fails_partway_buffers_none_of_it() {
    // simp724's T1 on #1511: two good writes, then a vector whose data doesn't fit its
    // dimension. The client gets InvalidArgument ("not applied"), so nothing may be buffered.
    let s = Server::start("", limits());
    let mut rows = txn_rows("p1", b"1");
    rows.as_array_mut()
        .unwrap()
        .extend(txn_rows("p2", b"2").as_array().unwrap().iter().cloned());
    rows.as_array_mut().unwrap().push(json!({
        "row": {"table": "t", "row": b64(b"p3")},
        "writes": [{"put": {"family": "f", "qualifier": b64(b"q"),
            "value": {"vector": {"element": "ELEMENT_F32", "dim": 2, "data": b64(&[0; 3])}}}}]
    }));
    for rpc in ["Transactions/Write", "Transactions/Commit"] {
        let id = begin(&s, None);
        let (status, e) = s.rpc(rpc, &json!({"txn_id": id, "rows": rows}), None);
        assert_eq!(
            (status, e["code"].as_str()),
            (400, Some("InvalidArgument")),
            "{rpc}: {e}"
        );
        // The session is still open, and a Commit with no rows commits nothing of it.
        let (status, body) = s.rpc("Transactions/Commit", &json!({"txn_id": id}), None);
        assert_eq!(status, 200, "{rpc}: {body}");
        assert_eq!(cell(&s, "p1", None), None, "{rpc}");
        assert_eq!(cell(&s, "p2", None), None, "{rpc}");
    }
}

// ---- Vectors.BulkLoad (D313 ticket 7b2) ----

/// A BulkLoad message of `n` vectors of `dim` f32s, ids `v{first..}`; the first message
/// names the index.
fn bulk_message(
    index: &str,
    expected: u64,
    first: usize,
    n: usize,
    dim: usize,
) -> proto::VectorBulkLoadRequest {
    let items = (first..first + n)
        .map(|i| {
            let mut v = vec![0.0f32; dim];
            v[0] = i as f32;
            proto::VectorItem {
                id: format!("v{i:05}").into_bytes().into(),
                vector: Some(proto::Vector {
                    element: proto::Element::F32 as i32,
                    dim: dim as u32,
                    data: v
                        .iter()
                        .flat_map(|x| x.to_le_bytes())
                        .collect::<Vec<u8>>()
                        .into(),
                }),
            }
        })
        .collect();
    proto::VectorBulkLoadRequest {
        index: index.into(),
        expected,
        items,
    }
}

fn vectors_client(s: &Server) -> proto::vectors_client::VectorsClient<tonic::transport::Channel> {
    let channel = s.block_on(async {
        tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap()
    });
    proto::vectors_client::VectorsClient::new(channel)
}

#[test]
fn a_bulk_load_fills_an_empty_index_over_grpc_and_http() {
    let s = Server::start("", limits());
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    // gRPC: three messages; only the first names the index.
    let mut client = vectors_client(&s);
    let messages = vec![
        bulk_message("ix", 300, 0, 100, 2),
        bulk_message("", 0, 100, 100, 2),
        bulk_message("", 0, 200, 100, 2),
    ];
    s.block_on(client.bulk_load(tonic::codegen::tokio_stream::iter(messages)))
        .unwrap();
    assert_eq!(verified(&s, "ix"), json!("300"));
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": f32_query(&[0.0, 0.0]), "k": 2}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"v00000"), b64(b"v00001")]);
    // The index isn't empty now: the crate refuses a second load.
    let again =
        s.block_on(
            client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                "ix", 1, 0, 1, 2,
            )])),
        );
    assert_eq!(again.unwrap_err().code(), tonic::Code::InvalidArgument);

    // HTTP: NDJSON, the gRPC messages as JSON lines, blank lines skipped.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let ndjson = |lines: &[proto::VectorBulkLoadRequest]| {
        let mut body = String::new();
        for m in lines {
            body.push_str(&serde_json::to_string(m).unwrap());
            body.push_str("\n\n");
        }
        body
    };
    let post = |body: &[u8]| {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut resp = agent
            .post(format!("{}/v1/Vectors/BulkLoad", s.url))
            .header("Content-Type", "application/x-ndjson")
            .send(body)
            .unwrap();
        let status = resp.status().as_u16();
        let body: Json = resp.body_mut().read_json().unwrap_or(Json::Null);
        (status, body)
    };
    let body = ndjson(&[
        bulk_message("ix2", 50, 0, 30, 2),
        bulk_message("", 0, 30, 20, 2),
    ]);
    let (status, r) = post(body.as_bytes());
    assert_eq!(status, 200, "{r}");
    assert_eq!(verified(&s, "ix2"), json!("50"));

    // Refused: an empty body, an unknown index, a line past --max-request-bytes (with no
    // newline, so it can't be split), and a vector of another element type, after which
    // the vectors before it are loaded.
    let (_, e) = post(b"");
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    let (_, e) = post(ndjson(&[bulk_message("nope", 1, 0, 1, 2)]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("TableNotFound"), "{e}");
    let (_, e) = post(&vec![b' '; (1 << 20) + 2]);
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix3");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut mixed = bulk_message("ix3", 3, 0, 3, 2);
    mixed.items[2].vector = Some(proto::Vector {
        element: proto::Element::I8 as i32,
        dim: 2,
        data: vec![0u8; 2].into(),
    });
    let (_, e) = post(ndjson(&[mixed]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    assert_eq!(verified(&s, "ix3"), json!("2"));
    // A vector of another dimension in a later message (ai1249's M1): the server ends the
    // input there, so the vectors before it are written and searchable, not dropped with
    // the crate's waiting batches.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix4");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut second = bulk_message("", 0, 50, 12, 2);
    second.items[10] = bulk_message("", 0, 60, 1, 3).items.remove(0);
    let (_, e) = post(ndjson(&[bulk_message("ix4", 62, 0, 50, 2), second]).as_bytes());
    assert_eq!(e["code"].as_str(), Some("InvalidArgument"), "{e}");
    assert!(
        e["message"]
            .as_str()
            .unwrap()
            .contains("vectors before it were loaded"),
        "{e}"
    );
    assert_eq!(verified(&s, "ix4"), json!("60"));
    let (_, r) = s.rpc(
        "Vectors/Search",
        &json!({"index": "ix4", "query": f32_query(&[59.0, 0.0]), "k": 1}),
        None,
    );
    assert_eq!(hit_ids(&r), [b64(b"v00059")]);
    // Every message's budget came back, and every load's reservation of
    // --bulk-load-bytes, whether it succeeded or failed (the audit's F7).
    assert_eq!(
        s.shared.router.service.inflight_available(),
        limits().inflight_bytes
    );
    assert_bulk_budget_free(&s);
}

/// A load that failed after loading part of its vectors says it isn't safe to send again,
/// and what it loaded (the audit's P2-C9).
fn assert_not_retry_safe(err: &tonic::Status) {
    let detail = errors::ApiError::from_status(err).expect("a detail");
    assert!(!detail.detail().retry_safe, "{err:?}");
    assert!(err.message().contains("were loaded"), "{err:?}");
}

/// Every load's reservation of `--bulk-load-bytes` is back, and the gauge says none.
fn assert_bulk_budget_free(s: &Server) {
    let service = &s.shared.router.service;
    assert_eq!(
        service.bulk_budget_available(),
        limits().bulk_load_bytes.div_ceil(1024)
    );
    assert_eq!(
        service
            .part()
            .unwrap()
            .bulk
            .gauges
            .reserved
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn a_bulk_load_over_bulk_load_bytes_is_refused_before_it_starts() {
    // The audit's F7: `expected` sizes the crate's sample and its lists, so a client could
    // make the server hold without bound. A load whose worst case is over --bulk-load-bytes
    // is refused before it takes anything, naming the bytes and the flag.
    let s = Server::start("", limits());
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    let mut client = vectors_client(&s);
    for expected in [u64::MAX, 1 << 40] {
        let err = s
            .block_on(
                client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                    "ix", expected, 0, 10, 2,
                )])),
            )
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
        assert!(
            err.message()
                .contains("raise --bulk-load-bytes to at least"),
            "{err:?}"
        );
        assert_eq!(verified(&s, "ix"), json!("0"), "nothing was loaded");
    }
    assert_bulk_budget_free(&s);
    // The same index loads once `expected` fits.
    s.block_on(
        client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
            "ix", 10, 0, 10, 2,
        )])),
    )
    .unwrap();
    assert_eq!(verified(&s, "ix"), json!("10"));
    assert_bulk_budget_free(&s);

    // A smaller --bulk-load-bytes refuses even the sample of a small load: 50,000 2-dim
    // vectors hold about 7 MB.
    let mut l = limits();
    l.bulk_load_bytes = 1 << 20;
    let s = Server::start("", l);
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    let mut client = vectors_client(&s);
    let err = s
        .block_on(
            client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                "ix", 10, 0, 10, 2,
            )])),
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
    assert!(
        err.message().contains("its sample of 50000 vectors"),
        "{err:?}"
    );
}

#[test]
fn a_bulk_load_stops_past_what_its_expected_sized_its_lists_for() {
    // The audit's P2-F2: `expected` sized the reservation but not the load, so `expected: 0`
    // put every vector in one list without bound, and the rebalance's split then read that
    // list whole. A load now stops once it has sent its lists' split length (lists × 2 ×
    // target), the vectors before it loaded. Here lists of 8 and `expected` 0 make one list,
    // so 16 vectors.
    let s = Server::start("", limits());
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["target_list_len"] = json!(8);
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut client = vectors_client(&s);
    let err = s
        .block_on(client.bulk_load(tonic::codegen::tokio_stream::iter(vec![
            bulk_message("ix", 0, 0, 50, 2),
            bulk_message("", 0, 50, 50, 2),
        ])))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err:?}");
    assert!(
        err.message().contains("sent more than 16 vectors"),
        "{err:?}"
    );
    assert_eq!(
        verified(&s, "ix"),
        json!("16"),
        "the vectors before it are loaded"
    );
    assert_bulk_budget_free(&s);
    // An honest `expected` loads all it declared, and up to what its lists hold: 40 makes 5
    // lists of up to 16, so 80.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    spec["target_list_len"] = json!(8);
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    s.block_on(client.bulk_load(tonic::codegen::tokio_stream::iter(vec![
        bulk_message("ix2", 40, 0, 50, 2),
        bulk_message("", 0, 50, 30, 2),
    ])))
    .unwrap();
    assert_eq!(verified(&s, "ix2"), json!("80"));
}

#[test]
fn a_768_dim_load_fits_the_default_bulk_load_bytes() {
    // The default 1 GiB holds a 768-dim f32 load's sample (about 315 MB) and its pending
    // batches up to about 1,300 lists. The reservation is the load's worst case while it
    // runs, and comes back when it ends.
    assert_eq!(limits().bulk_load_bytes, 1 << 30);
    let s = Server::start("", limits());
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["spec"]["dim"] = json!(768);
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut client = vectors_client(&s);
    let messages: Vec<_> = (0..4)
        .map(|i| bulk_message(if i == 0 { "ix" } else { "" }, 400, i * 100, 100, 768))
        .collect();
    s.block_on(client.bulk_load(tonic::codegen::tokio_stream::iter(messages)))
        .unwrap();
    assert_eq!(verified(&s, "ix"), json!("400"));
    assert_bulk_budget_free(&s);
}

#[test]
fn a_bulk_load_whose_client_stalls_ends_after_stream_idle() {
    // The audit's F7: a client that sends one message and then nothing held the one load
    // slot for its connection's life. Past --stream-idle-ms the load ends there, what it
    // took loaded, and the slot and the budget are free for the next.
    let mut l = limits();
    l.stream_idle = Duration::from_millis(300);
    let s = Server::start("", l);
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    let messages = tonic::codegen::tokio_stream::StreamExt::chain(
        tonic::codegen::tokio_stream::iter(vec![bulk_message("ix", 100, 0, 10, 2)]),
        tonic::codegen::tokio_stream::pending(),
    );
    let mut client = vectors_client(&s);
    let started = std::time::Instant::now();
    let err = s.block_on(client.bulk_load(messages)).unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded, "{err:?}");
    assert!(err.message().contains("--stream-idle-ms"), "{err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(verified(&s, "ix"), json!("10"), "what it took is loaded");
    // Part of it was applied, so it isn't safe to send again (the audit's P2-C9): BulkLoad
    // needs an empty index.
    assert_not_retry_safe(&err);
    assert_bulk_budget_free(&s);
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut second = vectors_client(&s);
    s.block_on(
        second.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
            "ix2", 1, 0, 1, 2,
        )])),
    )
    .unwrap();
    assert_eq!(verified(&s, "ix2"), json!("1"));
}

#[test]
fn a_slow_loader_holds_back_a_fast_client() {
    // The coordinator on 7b2, and perf287's (A): a loader that stops after its first vector
    // stops the server reading the stream, so the client's sends stall, and the server holds
    // the budget of two messages at most.
    let (started_tx, started) = std::sync::mpsc::channel::<()>();
    let (gate, gate_rx) = std::sync::mpsc::channel::<()>();
    let started_tx = std::sync::Mutex::new(started_tx);
    let gate_rx = std::sync::Mutex::new(gate_rx);
    let s = Server::start_custom("", limits(), 1, move |mut svc| {
        // Only the first load is held.
        let held = std::sync::atomic::AtomicBool::new(false);
        svc.part_mut().bulk.loader = Arc::new(move |index, expected, input| {
            if held.swap(true, Ordering::Relaxed) {
                return super::bulk::load(index, expected, input);
            }
            let first = input.next();
            started_tx.lock().unwrap().send(()).unwrap();
            let _ = gate_rx.lock().unwrap().recv();
            let mut rest = first.into_iter().chain(input);
            super::bulk::load(index, expected, &mut rest)
        });
        svc
    });
    // Rebound after the server, so a failed assertion drops the gate first: the server's
    // drop waits for the loader thread, which then ends instead of waiting on the gate.
    let gate = gate;
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["spec"]["dim"] = json!(1024);
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    // 200 messages of 16 vectors of 1,024 f32s: 64 KiB each, 12.8 MiB in all.
    const MESSAGES: usize = 200;
    const EACH: usize = 16;
    let message_bytes = bulk_message("", 0, 0, EACH, 1024).encoded_len();
    let sent = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&sent);
    let messages = tonic::codegen::tokio_stream::StreamExt::map(
        tonic::codegen::tokio_stream::iter(0..MESSAGES),
        move |i| {
            counted.fetch_add(1, Ordering::Relaxed);
            let index = if i == 0 { "ix" } else { "" };
            bulk_message(index, (MESSAGES * EACH) as u64, i * EACH, EACH, 1024)
        },
    );
    let mut client = vectors_client(&s);
    let load = s
        .runtime
        .as_ref()
        .unwrap()
        .spawn(async move { client.bulk_load(messages).await });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let stalled = sent.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(
        sent.load(Ordering::Relaxed),
        stalled,
        "the client's sends stalled"
    );
    assert!(
        (stalled as usize) < MESSAGES / 2,
        "the transport's windows hold a few messages, not the stream: {stalled} sent"
    );
    let service = &s.shared.router.service;
    let reserved = limits().inflight_bytes - service.inflight_available();
    assert!(
        reserved <= 2 * message_bytes + 64,
        "the server holds two messages' budget at most: {reserved} bytes, {message_bytes} each"
    );
    let g = &service.part().unwrap().bulk.gauges;
    assert_eq!(g.phase.load(Ordering::Relaxed), 1, "receiving");
    assert!(g.vectors.load(Ordering::Relaxed) >= EACH as u64);
    // A second load waits for the one-load slot, here past its client's deadline.
    let mut spec2 = index_spec("REBALANCE_INLINE");
    spec2["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec2, None).0, 200);
    let mut second = vectors_client(&s);
    let mut req = tonic::Request::new(tonic::codegen::tokio_stream::iter(vec![bulk_message(
        "ix2", 1, 0, 1, 2,
    )]));
    req.set_timeout(Duration::from_millis(300));
    assert!(
        s.block_on(second.bulk_load(req)).is_err(),
        "the slot is taken"
    );
    // Released, the loader takes the rest, and the client sends it.
    gate.send(()).unwrap();
    s.block_on(load).unwrap().unwrap();
    assert_eq!(sent.load(Ordering::Relaxed) as usize, MESSAGES);
    assert_eq!(verified(&s, "ix"), json!((MESSAGES * EACH).to_string()));
    assert_eq!(service.inflight_available(), limits().inflight_bytes);
    assert_eq!(g.phase.load(Ordering::Relaxed), 0, "no load");
    // The slot is free again, at once.
    s.block_on(
        second.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
            "ix2", 1, 0, 1, 2,
        )])),
    )
    .unwrap();
    assert_eq!(verified(&s, "ix2"), json!("1"));
}

#[test]
fn a_shutdown_stops_a_running_load_between_messages() {
    // The coordinator on #1580: a load can't hold the server past its grace period. Here
    // the client sends one message, then nothing: the loader waits for the next, and the
    // shutdown wakes it. The load ends with what it took, and its client hears Closed.
    let s = Server::start("", limits());
    assert_eq!(
        s.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None)
            .0,
        200
    );
    let messages = tonic::codegen::tokio_stream::StreamExt::chain(
        tonic::codegen::tokio_stream::iter(vec![bulk_message("ix", 100, 0, 10, 2)]),
        tonic::codegen::tokio_stream::pending(),
    );
    let mut client = vectors_client(&s);
    let load = s
        .runtime
        .as_ref()
        .unwrap()
        .spawn(async move { client.bulk_load(messages).await });
    let bulk = &s.shared.router.service.part().unwrap().bulk;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while bulk.gauges.vectors.load(Ordering::Relaxed) < 10 {
        assert!(
            std::time::Instant::now() < deadline,
            "the load never took the message"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    bulk.stop();
    let err = s.block_on(load).unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert!(err.message().contains("interrupted"), "{err:?}");
    assert_eq!(verified(&s, "ix"), json!("10"), "what it took is loaded");
    assert_not_retry_safe(&err);
    assert_eq!(bulk.gauges.phase.load(Ordering::Relaxed), 0, "no load");
    // No load starts once the server is stopping.
    let mut spec = index_spec("REBALANCE_INLINE");
    spec["name"] = json!("ix2");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let mut client = vectors_client(&s);
    let err = s
        .block_on(
            client.bulk_load(tonic::codegen::tokio_stream::iter(vec![bulk_message(
                "ix2", 1, 0, 1, 2,
            )])),
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert_eq!(verified(&s, "ix2"), json!("0"));
    // Nothing was applied: this one stays safe to send again.
    let detail = errors::ApiError::from_status(&err).expect("a detail");
    assert!(detail.detail().retry_safe, "{err:?}");
    assert_eq!(
        s.shared.router.service.inflight_available(),
        limits().inflight_bytes
    );
}

/// #1588: dropping a vector index's table evicts its cached handle and stops its background
/// rebalancer. A search then answers `TableNotFound`, and a new index made under the name
/// (with `if_missing`, which a cached handle used to answer at once) is the one served.
#[test]
fn dropping_an_index_s_table_evicts_its_handle_and_its_rebalancer() {
    let s = Server::start("", limits());
    let indexes = &s.shared.router.service.part().unwrap().indexes;
    let insert = |id: &[u8], y: f32| {
        let (status, e) = s.rpc(
            "Vectors/Insert",
            &json!({"index": "ix", "id": b64(id), "vector": f32_query(&[0.0, y])}),
            None,
        );
        assert_eq!(status, 200, "{e}");
    };
    let search = || {
        s.rpc(
            "Vectors/Search",
            &json!({"index": "ix", "query": f32_query(&[0.0, 0.0]), "k": 10}),
            None,
        )
    };
    let spec = index_spec("REBALANCE_BACKGROUND");
    assert_eq!(s.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    insert(b"old", 1.0);
    let (status, r) = search();
    assert_eq!((status, hit_ids(&r)), (200, vec![b64(b"old")]), "{r}");
    assert_eq!(indexes.rebalancing(), 1);

    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "ix"}), None);
    assert_eq!(status, 200, "{e}");
    let (status, r) = search();
    assert_eq!(
        (status, r["code"].as_str()),
        (404, Some("TableNotFound")),
        "{r}"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while indexes.rebalancing() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the dropped index's rebalancer still runs"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Made again: `if_missing` opens the new index, which the calls then use.
    let mut again = spec.clone();
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Vectors/CreateIndex", &again, None).0, 200);
    insert(b"new", 2.0);
    let (status, r) = search();
    assert_eq!((status, hit_ids(&r)), (200, vec![b64(b"new")]), "{r}");
}
