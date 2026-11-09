use super::*;

// ---- Transactions and Snapshots (D313 ticket 4) ----

#[test]
fn a_commit_sent_again_answers_the_first_ones_outcome() {
    let s = Server::start("", limits());
    let id = begin(&s, None);
    let (status, body) = s.rpc(
        "Transactions/Write",
        &json!({"txn_id": id, "rows": txn_rows("a", b"1")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    let commit = json!({"txn_id": id, "rows": txn_rows("b", b"2")});
    let (status, first) = s.rpc("Transactions/Commit", &commit, None);
    assert_eq!(status, 200, "{first}");
    // The reply is lost; the client sends Commit again, rows and all: the tombstone answers
    // the first commit's outcome, and nothing commits twice (simp724's B1 on D313).
    let (status, again) = s.rpc("Transactions/Commit", &commit, None);
    assert_eq!((status, &again), (200, &first));
    assert_eq!(cell(&s, "a", None), Some(b64(b"1")));
    assert_eq!(cell(&s, "b", None), Some(b64(b"2")));
    // Any other call names the outcome without claiming one of its own.
    for (rpc, body) in [
        ("Transactions/Abort", json!({"txn_id": id})),
        (
            "Transactions/Get",
            json!({"txn_id": id, "table": "t", "row": b64(b"a"), "family": "f", "qualifier": b64(b"q")}),
        ),
    ] {
        let (status, e) = s.rpc(rpc, &body, None);
        assert_eq!(
            (status, e["code"].as_str()),
            (409, Some("TransactionFinished")),
            "{e}"
        );
        assert!(
            e["message"]
                .as_str()
                .unwrap()
                .contains("committed at seqno"),
            "{e}"
        );
    }

    // A conflict is recorded the same way.
    let id = begin(&s, None);
    let get = json!({"txn_id": id, "table": "t", "row": b64(b"a"), "family": "f", "qualifier": b64(b"q")});
    let (status, read) = s.rpc("Transactions/Get", &get, None);
    assert_eq!(status, 200, "{read}");
    assert_eq!(
        s.post(
            "Mutate",
            &put("a", "f", "q", json!({"bytes": b64(b"other")})),
            None
        )
        .0,
        200
    );
    let commit = json!({"txn_id": id, "rows": txn_rows("a", b"mine")});
    for _ in 0..2 {
        let (status, e) = s.rpc("Transactions/Commit", &commit, None);
        assert_eq!((status, e["code"].as_str()), (409, Some("Conflict")), "{e}");
    }
    assert_eq!(cell(&s, "a", None), Some(b64(b"other")));
}

#[test]
fn a_commit_whose_client_went_away_still_lands_and_is_recorded() {
    let s = Server::start("", limits());
    let service = &s.shared.router.service;
    let caller = Caller::anonymous();
    // The table is opened once first, so the commit reaches its spawn on the first poll.
    assert!(cell(&s, "gone", None).is_none());
    let id = match s
        .block_on(service.call(ServiceOp::Begin(Default::default()), &caller, None))
        .unwrap()
    {
        super::service::ServiceReply::Begin(b) => b.txn_id,
        _ => unreachable!(),
    };
    let rows: Vec<proto::RowWrites> = serde_json::from_value(txn_rows("gone", b"landed")).unwrap();
    let commit = ServiceOp::Commit(proto::TxnCommitRequest {
        txn_id: id.clone(),
        rows,
    });
    s.block_on(async {
        let call = service.call(commit, &caller, None);
        tokio::select! {
            biased;
            _ = call => panic!("the commit answered before its first await"),
            () = std::future::ready(()) => {}
        }
    });
    // The request is gone; the commit isn't, and the session holds its outcome: a Commit
    // sent again gets SessionBusy while it runs, then the tombstone.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let info = loop {
        let again = ServiceOp::Commit(proto::TxnCommitRequest {
            txn_id: id.clone(),
            rows: Vec::new(),
        });
        match s.block_on(service.call(again, &caller, None)) {
            Ok(super::service::ServiceReply::Commit(r)) => break r.commit.unwrap(),
            Err(e) if e.code == "SessionBusy" && std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            other => panic!("{other:?}"),
        }
    };
    assert!(info.seqno > 0);
    assert_eq!(cell(&s, "gone", None), Some(b64(b"landed")));
    wait_for("the lost reply is counted", || {
        s.shared.metrics.replies_lost.load(Ordering::Relaxed) == 1
    });
}

#[test]
fn transaction_expired_comes_only_before_a_commit() {
    // The test drives the expiry itself, with `sweep` at a chosen instant: the reaper runs
    // on the wall clock and, with a short lease on a slow runner, can reap the expired
    // marker before the test reads it (#1531). With a lease this long, it never does. The
    // sweep's instant is past both the idle lease and `limits()`'s 300 s transaction
    // lifetime, so either would expire them.
    let mut l = limits();
    l.leases.txn_idle = Duration::from_secs(600);
    let s = Server::start("", l);
    let idle = begin(&s, None);
    let committed = begin(&s, None);
    let (status, body) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": committed, "rows": txn_rows("c", b"1")}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    let lapsed = std::time::Instant::now() + Duration::from_secs(601);
    assert_eq!(
        s.shared.router.service.sessions.sweep(lapsed),
        2,
        "both lapse"
    );
    // The idle one expired before any Commit: safe to start again from Begin.
    let (status, e) = s.rpc("Transactions/Commit", &json!({"txn_id": idle}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (409, Some("TransactionExpired")),
        "{e}"
    );
    assert_eq!(e["retry_from"], "RETRY_FROM_BEGIN", "{e}");
    // The committed one's tombstone expired too: NotFound, which for a client that sent
    // Commit means "outcome unknown", never TransactionExpired ("not committed").
    let (status, e) = s.rpc("Transactions/Commit", &json!({"txn_id": committed}), None);
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    assert_eq!(cell(&s, "c", None), Some(b64(b"1")));
}

#[test]
fn sessions_belong_to_their_token_and_read_tokens_only_read() {
    let s = Server::start("read r3ad\nwrite wr1te\n", limits());
    let mine = begin(&s, Some("wr1te"));
    // Another token's use of the id is NotFound, never PermissionDenied: ids can't be probed.
    let (status, e) = s.rpc("Transactions/Abort", &json!({"txn_id": mine}), Some("r3ad"));
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    // A read token may begin, read, and commit a transaction with no writes...
    let reader = begin(&s, Some("r3ad"));
    let (status, body) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": reader}),
        Some("r3ad"),
    );
    assert_eq!(status, 200, "{body}");
    // ...but not write in one.
    let reader = begin(&s, Some("r3ad"));
    for (rpc, body) in [
        (
            "Transactions/Write",
            json!({"txn_id": reader, "rows": txn_rows("x", b"1")}),
        ),
        (
            "Transactions/Commit",
            json!({"txn_id": reader, "rows": txn_rows("x", b"1")}),
        ),
    ] {
        let (status, e) = s.rpc(rpc, &body, Some("r3ad"));
        assert_eq!(
            (status, e["code"].as_str()),
            (403, Some("PermissionDenied")),
            "{rpc}: {e}"
        );
    }
    let (status, body) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": reader}),
        Some("r3ad"),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": mine}),
        Some("wr1te"),
    );
    assert_eq!(status, 200, "{body}");
    // Aborted: the id is gone.
    let (status, _) = s.rpc(
        "Transactions/Abort",
        &json!({"txn_id": mine}),
        Some("wr1te"),
    );
    assert_eq!(status, 404);
}

#[test]
fn reads_at_a_pinned_snapshot_agree_until_it_is_released() {
    let s = Server::start("", limits());
    let mut c = s.grpc();
    seed_rows(&s, &mut c);
    let (status, opened) = s.rpc("Snapshots/Open", &json!({}), None);
    assert_eq!(status, 200, "{opened}");
    let id = opened["snapshot_id"].as_str().unwrap().to_owned();
    assert!(opened["seqno"].as_str().unwrap().parse::<u64>().unwrap() > 0);
    // Changes after the snapshot: a row deleted, one added.
    assert_eq!(
        s.post(
            "Mutate",
            &put("r00", "f", "q", json!({"bytes": b64(b"new")})),
            None
        )
        .0,
        200
    );
    assert_eq!(
        s.post(
            "Mutate",
            &put("r99", "f", "q0", json!({"bytes": b64(b"new")})),
            None
        )
        .0,
        200
    );
    // A Get and a Scan at the snapshot see it as it was, over every page.
    let mut get = json!({"table": "t", "row": b64(b"r00"), "family": "f", "qualifier": b64(b"q0"), "snapshot_id": id});
    let (_, body) = s.post("Get", &get, None);
    assert_eq!(body["cell"]["value"], b64(&[b'v'; 400]), "{body}");
    let mut req = scan_request(1000, 0, Vec::new());
    req.selection = Some(proto::Selection {
        snapshot_id: id.clone(),
        ..Default::default()
    });
    let pages = grpc_scan(&s, &mut c, req.clone());
    assert_eq!(join(&pages), whole_rows(30), "r99 isn't in the snapshot");
    // A Begin's snapshot seqno can be compared with the pinned one's.
    let (_, b) = s.rpc("Transactions/Begin", &json!({}), None);
    assert!(
        b["snapshot_seqno"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > opened["seqno"].as_str().unwrap().parse::<u64>().unwrap()
    );
    // Released: later reads at it are NotFound, including a stream's next page.
    let (status, _) = s.rpc("Snapshots/Release", &json!({"snapshot_id": id}), None);
    assert_eq!(status, 200);
    get["qualifier"] = json!(b64(b"q1"));
    let (status, e) = s.post("Get", &get, None);
    assert_eq!((status, e["code"].as_str()), (404, Some("NotFound")), "{e}");
    let err = s.block_on(c.scan(req)).unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound, "{err:?}");
    let text = s.shared.metrics.render(&s.shared.router.service.db);
    let mut text = text;
    super::service::render_sessions(&mut text, &s.shared.router.service.sessions);
    assert!(
        text.contains(r#"pigeonhole_serve_sessions{kind="transaction"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(r#"pigeonhole_serve_sessions{kind="snapshot"} 0"#),
        "{text}"
    );
}

#[test]
fn an_aggregate_vector_input_is_refused_as_each_path_always_refused_it() {
    // The coordinator on #1555's N1: a transaction decodes an aggregate's input before it
    // checks for a vector, so malformed vector elements get the element error; Mutate refuses
    // a vector input before decoding it. Both are InvalidArgument; the messages differ.
    let s = Server::start("", limits());
    let bad = json!({"vector": {"element": "ELEMENT_F32", "dim": 2, "data": b64(&[0; 3])}});
    let write = json!({"aggregate": {"family": "f", "qualifier": b64(b"q"), "input": bad}});
    let id = begin(&s, None);
    let rows = json!([{"row": {"table": "t", "row": b64(b"r")}, "writes": [write.clone()]}]);
    let (status, e) = s.rpc(
        "Transactions/Commit",
        &json!({"txn_id": id, "rows": rows}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        e["message"], "a vector of 2 elements needs 8 bytes of data, not 3",
        "the element error, as before: {e}"
    );
    let (status, e) = s.post(
        "Mutate",
        &json!({"writes": {"row": {"table": "t", "row": b64(b"r")}, "writes": [write]}}),
        None,
    );
    assert_eq!(
        (status, e["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{e}"
    );
    assert_eq!(
        e["message"], "an aggregate input is an i64, u64, f64 or bytes",
        "{e}"
    );
}
