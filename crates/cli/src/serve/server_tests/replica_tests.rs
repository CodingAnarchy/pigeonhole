use super::*;

// ---- Replication (#1436) ----

fn get_value(s: &Server, table: &str, row: &str, token: Option<&str>) -> Json {
    let (status, body) = s.post(
        "Get",
        &json!({"table": table, "row": b64(row.as_bytes()), "family": "f", "qualifier": b64(b"q")}),
        token,
    );
    assert_eq!(status, 200, "{body}");
    body["cell"]["value"].clone()
}

#[test]
fn a_replica_bootstraps_follows_and_refuses_writes() {
    let p = Server::start("admin s3cret", limits());
    let tok = Some("s3cret");
    for i in 0..20 {
        let value = json!({"bytes": b64(format!("v{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), tok)
                .0,
            200
        );
    }
    let r = Server::start_replica(test_dir(), &p.url, Some("s3cret"));
    wait_replica(&r, "caught up after its bootstrap", caught_up(&p, tok));
    assert_eq!(get_value(&r, "t", "r7", None), b64(b"v7"));

    // Commits after the bootstrap follow, a new table and its writes in commit order.
    for i in 20..40 {
        let value = json!({"bytes": b64(format!("v{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), tok)
                .0,
            200
        );
    }
    let (status, body) = p.rpc(
        "Schema/CreateTable",
        &json!({"name": "u", "families": [{"name": "f"}]}),
        tok,
    );
    assert_eq!(status, 200, "{body}");
    let mut w = put("x", "f", "q", json!({"bytes": b64(b"in u")}));
    w["writes"]["row"]["table"] = json!("u");
    assert_eq!(p.post("Mutate", &w, tok).0, 200);
    // The primary's durable feed: a sync makes them durable at once.
    assert_eq!(p.rpc("Admin/Flush", &json!({}), tok).0, 200);
    let info = wait_replica(&r, "the later commits", caught_up(&p, tok));
    assert_eq!(info["primary"], p.url);
    assert_eq!(get_value(&r, "t", "r39", None), b64(b"v39"));
    assert_eq!(get_value(&r, "u", "x", None), b64(b"in u"));
    // A table dropped and made again on the primary: the replica's reads (whose handle for
    // the name was cached) see the new one.
    assert_eq!(p.rpc("Schema/DropTable", &json!({"name": "u"}), tok).0, 200);
    let (status, body) = p.rpc(
        "Schema/CreateTable",
        &json!({"name": "u", "families": [{"name": "f"}]}),
        tok,
    );
    assert_eq!(status, 200, "{body}");
    let mut w = put("x2", "f", "q", json!({"bytes": b64(b"in the new u")}));
    w["writes"]["row"]["table"] = json!("u");
    assert_eq!(p.post("Mutate", &w, tok).0, 200);
    assert_eq!(p.rpc("Admin/Flush", &json!({}), tok).0, 200);
    wait_replica(&r, "the new table", caught_up(&p, tok));
    assert_eq!(get_value(&r, "u", "x2", None), b64(b"in the new u"));
    assert_eq!(get_value(&r, "u", "x", None), Json::Null);

    // Writes and schema changes are refused, naming the primary.
    let (status, body) = r.post(
        "Mutate",
        &put("y", "f", "q", json!({"bytes": b64(b"no")})),
        None,
    );
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("ReadOnly")),
        "{body}"
    );
    assert!(body["message"].as_str().unwrap().contains(&p.url), "{body}");
    let (status, body) = r.rpc("Schema/DropTable", &json!({"name": "u"}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("ReadOnly")),
        "{body}"
    );
    // A transaction's Commit with writes reaches the engine's gate, and says the same.
    let id = begin(&r, None);
    let (status, body) = r.rpc(
        "Transactions/Commit",
        &json!({"txn_id": id, "rows": txn_rows("z", b"no")}),
        None,
    );
    assert_eq!(status, 409, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("replica"),
        "{body}"
    );

    // The lag gauges.
    let (_, m) = r.rpc("Admin/Metrics", &json!({}), None);
    let text = m["text"].as_str().unwrap();
    assert!(text.contains("pigeonhole_replica_following 1"), "{text}");
    assert!(text.contains("pigeonhole_replica_lag_commits"), "{text}");

    // Every write RPC, whatever its body: the gate is the call's scope (cli692's F2).
    for rpc in [
        "Data/Mutate",
        "Data/CheckAndMutate",
        "Data/Batch",
        "Data/MutateReturning",
        "Transactions/Write",
        "Schema/CreateTable",
        "Schema/DropTable",
        "Vectors/CreateIndex",
        "Vectors/Insert",
        "Vectors/Delete",
        "Vectors/BulkLoad",
        "Text/CreateIndex",
        "Text/Put",
        "Text/Delete",
        "Text/Rebuild",
        "Memory/Remember",
        "Memory/MarkUsed",
        "Memory/Supersede",
        "Memory/Link",
        "Memory/Forget",
    ] {
        let (status, body) = r.rpc(rpc, &json!({}), None);
        assert_eq!(
            (status, body["code"].as_str()),
            (409, Some("ReadOnly")),
            "{rpc}: {body}"
        );
    }

    // Caught up means no lag: the primary reports its durable frontier, which a replica can
    // reach, not its visible seqno (cli692's F5).
    let info = wait_replica(&r, "caught up again", caught_up(&p, tok));
    assert!(info.get("lag_commits").is_none(), "{info}");

    // A replica is no bootstrap source and can't be followed (no cascades).
    let (status, body) = r.rpc("Admin/BackupDownload", &json!({"replica": true}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{body}"
    );
}

#[test]
fn a_bootstrap_clears_a_failed_one_s_side_files() {
    // #1748: a bootstrap that failed after its copy's clean close (or a process that died
    // mid-bootstrap) left the copy's WAL and feed sidecars beside the database
    // (`t.phdb.bootstrap-wal-*`, `-feed`: a change feed's clean close keeps its WAL). Every
    // later bootstrap opened its fresh copy beside them and failed with "WAL segment belongs
    // to another database", so the replica retried for ever. The bootstrap now removes
    // every file of the side copy (`Pigeonhole::database_files`) before it downloads.
    let p = Server::start("admin s3cret", limits());
    let tok = Some("s3cret");
    for i in 0..5 {
        let value = json!({"bytes": b64(format!("v{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), tok)
                .0,
            200
        );
    }
    // Another database's side copy, closed cleanly with a feed, its main file then removed
    // as the old failure path did: its WAL and feed sidecars stay.
    let dir = test_dir();
    let side = dir.join("t.phdb.bootstrap");
    let other = Pigeonhole::open(
        &side,
        Options::default()
            .create_if_missing(true)
            .shards(1)
            .change_feed(pigeonhole::FeedRetention::bytes(64 << 20)),
    )
    .unwrap();
    other
        .table("z")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    other.close().unwrap();
    std::fs::remove_file(&side).unwrap();
    let left = |dir: &std::path::Path| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("t.phdb.bootstrap"))
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        left(&dir),
        ["t.phdb.bootstrap-feed", "t.phdb.bootstrap-wal-0"],
        "the failed bootstrap's leftovers"
    );
    let r = Server::start_replica(dir.clone(), &p.url, tok);
    // The first bootstrap succeeds: the leftovers go before it opens its copy, so the
    // replica never shows `retrying` (a failed attempt sits there for its 100 ms pause,
    // longer than `wait_replica`'s 50 ms poll).
    let caught = caught_up(&p, tok);
    let mut states: Vec<String> = Vec::new();
    let info = wait_replica(&r, "caught up past a failed bootstrap's files", |info| {
        let state = info["state"].as_str().unwrap_or_default().to_owned();
        if states.last() != Some(&state) {
            states.push(state);
        }
        caught(info)
    });
    assert!(
        !states.iter().any(|s| s == "retrying"),
        "a bootstrap failed first: {states:?}, {info}"
    );
    assert_eq!(get_value(&r, "t", "r3", None), b64(b"v3"));
    assert!(
        left(&dir).is_empty(),
        "no side file is left: {:?}",
        left(&dir)
    );
}

#[test]
fn a_restarted_replica_resumes_and_a_new_primary_bootstraps_it_again() {
    let p = Server::start("", limits());
    for i in 0..10 {
        let value = json!({"bytes": b64(format!("a{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), None)
                .0,
            200
        );
    }
    let dir = test_dir();
    let mut r = Server::start_replica(dir.clone(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    r.keep_dir = true;
    drop(r);

    // Restarted, it resumes from its stored token: no second bootstrap, and what the primary
    // wrote meanwhile arrives.
    for i in 10..20 {
        let value = json!({"bytes": b64(format!("a{i}").as_bytes())});
        assert_eq!(
            p.post("Mutate", &put(&format!("r{i}"), "f", "q", value), None)
                .0,
            200
        );
    }
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    let downloads = || std::fs::read_dir(p.dir.join("backups")).map_or(0, |d| d.count());
    let before = downloads();
    let mut r = Server::start_replica(dir.clone(), &p.url, None);
    wait_replica(&r, "caught up after its restart", caught_up(&p, None));
    assert_eq!(get_value(&r, "t", "r15", None), b64(b"a15"));
    assert_eq!(downloads(), before, "a restart takes no bootstrap copy");
    r.keep_dir = true;
    drop(r);

    // Another database at a URL: the replica's token is another database's, so it
    // bootstraps again from the new one rather than apply its history.
    let q = Server::start("", limits());
    let value = json!({"bytes": b64(b"from q")});
    assert_eq!(q.post("Mutate", &put("q1", "f", "q", value), None).0, 200);
    let r = Server::start_replica(dir, &q.url, None);
    wait_replica(&r, "caught up with the new primary", caught_up(&q, None));
    assert_eq!(get_value(&r, "t", "q1", None), b64(b"from q"));
    assert_eq!(
        get_value(&r, "t", "r15", None),
        Json::Null,
        "the old copy is gone"
    );
}

#[test]
fn promote_makes_a_replica_take_writes() {
    let p = Server::start("", limits());
    let value = json!({"bytes": b64(b"v")});
    assert_eq!(p.post("Mutate", &put("r1", "f", "q", value), None).0, 200);
    // On a primary, Promote is refused.
    let (status, body) = p.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(
        (status, body["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{body}"
    );

    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let (status, body) = r.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(status, 200, "{body}");
    assert!(
        body["primary_token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "{body}"
    );
    // A primary now: writes are taken, and Info has no replica section.
    let value = json!({"bytes": b64(b"mine")});
    assert_eq!(r.post("Mutate", &put("r2", "f", "q", value), None).0, 200);
    assert_eq!(get_value(&r, "t", "r2", None), b64(b"mine"));
    let (_, info) = r.rpc("Admin/Info", &json!({}), None);
    assert!(info.get("replica").is_none(), "{info}");
    // It follows no more: the old primary's writes don't arrive.
    let value = json!({"bytes": b64(b"late")});
    assert_eq!(p.post("Mutate", &put("r3", "f", "q", value), None).0, 200);
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(get_value(&r, "t", "r3", None), Json::Null);
    let (status, _) = r.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(status, 409, "promoted once");
}

/// An IVF index made and written on the primary is searched and verified on a replica, and
/// the replica refuses its writes like any other (#1436 with #1573's index RPCs).
#[test]
fn a_replica_serves_ivf_index_reads_and_refuses_its_writes() {
    let p = Server::start("", limits());
    for (name, mode) in [("ix", "REBALANCE_INLINE"), ("bg", "REBALANCE_BACKGROUND")] {
        let mut spec = index_spec(mode);
        spec["name"] = json!(name);
        let (status, e) = p.rpc("Vectors/CreateIndex", &spec, None);
        assert_eq!(status, 200, "{e}");
        for (id, y) in [("a", 1.0f32), ("b", 2.0), ("c", 3.0)] {
            let (status, e) = p.rpc(
                "Vectors/Insert",
                &json!({"index": name, "id": b64(id.as_bytes()), "vector": f32_query(&[0.0, y])}),
                None,
            );
            assert_eq!(status, 200, "{id}: {e}");
        }
    }
    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    for name in ["ix", "bg"] {
        let q = f32_query(&[0.0, 0.0]);
        let (status, hits) = r.rpc(
            "Vectors/Search",
            &json!({"index": name, "query": q, "k": 2}),
            None,
        );
        assert_eq!(status, 200, "{name}: {hits}");
        assert_eq!(hit_ids(&hits), [b64(b"a"), b64(b"b")], "{name}");
        let (status, v) = r.rpc("Vectors/Verify", &json!({"index": name}), None);
        assert_eq!(status, 200, "{name}: {v}");
        for (rpc, body) in [
            (
                "Vectors/Insert",
                json!({"index": name, "id": b64(b"z"), "vector": f32_query(&[1.0, 1.0])}),
            ),
            ("Vectors/Delete", json!({"index": name, "id": b64(b"a")})),
        ] {
            let (status, e) = r.rpc(rpc, &body, None);
            assert_eq!(
                (status, e["code"].as_str()),
                (409, Some("ReadOnly")),
                "{rpc}: {e}"
            );
        }
    }
    let (status, e) = r.rpc("Vectors/CreateIndex", &index_spec("REBALANCE_INLINE"), None);
    assert_eq!((status, e["code"].as_str()), (409, Some("ReadOnly")), "{e}");
    // Writes on the primary after the index was opened on the replica still arrive.
    let (status, e) = p.rpc(
        "Vectors/Insert",
        &json!({"index": "ix", "id": b64(b"n"), "vector": f32_query(&[0.0, 0.5])}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    assert_eq!(p.rpc("Admin/Flush", &json!({}), None).0, 200);
    wait_replica(&r, "the later insert", caught_up(&p, None));
    let q = f32_query(&[0.0, 0.0]);
    let (_, hits) = r.rpc(
        "Vectors/Search",
        &json!({"index": "ix", "query": q, "k": 1}),
        None,
    );
    assert_eq!(hit_ids(&hits), [b64(b"n")]);
}

/// Two `Admin.Promote` calls at once: one promotes, the other is refused (cli692's F4). A
/// background index rebalances on a replica only once it's promoted, and the catalog
/// changes that drop the server's index handles don't pile up rebalancer threads (F3).
#[test]
fn concurrent_promotes_promote_once_and_rebalancers_start_after() {
    let p = Server::start("", limits());
    let mut spec = index_spec("REBALANCE_BACKGROUND");
    spec["name"] = json!("bg");
    assert_eq!(p.rpc("Vectors/CreateIndex", &spec, None).0, 200);
    let (status, e) = p.rpc(
        "Vectors/Insert",
        &json!({"index": "bg", "id": b64(b"a"), "vector": f32_query(&[0.0, 1.0])}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let search = |s: &Server| {
        let q = f32_query(&[0.0, 0.0]);
        let (status, hits) = s.rpc(
            "Vectors/Search",
            &json!({"index": "bg", "query": q, "k": 1}),
            None,
        );
        assert_eq!(status, 200, "{hits}");
        assert_eq!(hit_ids(&hits), [b64(b"a")]);
    };
    let indexes = &r.shared.router.service.part().unwrap().indexes;
    search(&r);
    assert_eq!(indexes.rebalancing(), 0, "no rebalancer on a replica");

    let statuses: Vec<(u16, Json)> = std::thread::scope(|scope| {
        let calls: Vec<_> = (0..2)
            .map(|_| scope.spawn(|| r.rpc("Admin/Promote", &json!({}), None)))
            .collect();
        calls.into_iter().map(|c| c.join().unwrap()).collect()
    });
    let ok = statuses.iter().filter(|(s, _)| *s == 200).count();
    assert_eq!(ok, 1, "exactly one promote: {statuses:?}");
    let refused = statuses.iter().find(|(s, _)| *s != 200).unwrap();
    assert_eq!(
        (refused.0, refused.1["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{refused:?}"
    );

    // A primary now: the index's rebalancer runs. Catalog changes forget the handles, and
    // each next search reopens it; the forgotten rebalancers end, so one is left.
    search(&r);
    assert_eq!(indexes.rebalancing(), 1);
    for _ in 0..3 {
        r.shared.router.service.tables_changed();
        search(&r);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while indexes.rebalancing() > 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "{} rebalancers after three forgets",
            indexes.rebalancing()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A replica's own feed subscribers when a bootstrap swaps a new copy in: their tokens are
/// the old copy's database's, and they end with an error rather than silently starting
/// over on the new one (cli692's N3). Driven as the replica's loop does it, on one server.
#[test]
fn a_swap_ends_the_old_copy_s_feed_streams_with_an_error() {
    let s = Server::start("", limits());
    let other = Server::start("", limits());
    write_row(&other, b"theirs");
    let db = &s.shared.router.service.part().unwrap().db;
    let side = s.dir.join("side.phdb");
    {
        let theirs = &other.shared.router.service.part().unwrap().db;
        let snap = theirs.durable_snapshot().unwrap();
        theirs.backup_at(&snap, &side).unwrap();
    }
    let mut events = direct_feed(&s, subscribe(&["t"], b""));
    write_row(&s, b"ours");
    let got = drain(&s, &mut events, Duration::from_millis(300));
    assert_eq!(got.len(), 1, "{got:?}");
    let token = got[0].2.clone();

    db.replace_with(&side).unwrap();
    s.shared.router.service.engine_replaced();

    // The open stream: an error, never an event of the new copy.
    let mut ended = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while ended.is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the stream never ended"
        );
        match next_event(&s, &mut events, Duration::from_millis(200)) {
            Some(Ok(e)) => assert!(commit_of(&e).is_none(), "an event after the swap: {e:?}"),
            Some(Err(e)) => ended = Some(e),
            None => {}
        }
    }
    let ended = ended.unwrap();
    // A resubscribe from the old token is refused the same way.
    let resumed = s.block_on(s.shared.router.service.call(
        ServiceOp::Subscribe(proto::SubscribeRequest {
            start: Some(proto::subscribe_request::Start::Resume(token.into())),
            ..subscribe(&["t"], b"")
        }),
        &Caller::anonymous(),
        None,
    ));
    let refused = match resumed {
        Err(e) => e,
        Ok(super::service::ServiceReply::Feed(mut events)) => {
            match next_event(&s, &mut events, Duration::from_secs(5)) {
                Some(Err(e)) => e,
                other => panic!("a resume from the old copy's token: {other:?}"),
            }
        }
        Ok(_) => unreachable!(),
    };
    eprintln!(
        "ended: {} {}; resume: {} {}",
        ended.code, ended.message, refused.code, refused.message
    );
    for e in [&ended, &refused] {
        assert!(
            ["FeedExpired", "InvalidArgument"].contains(&e.code.as_str()),
            "{}: {}",
            e.code,
            e.message
        );
    }
}

// ---- Read-your-writes on a replica (#1577) ----

/// A client that committed on the primary reads its write on a replica by sending the
/// commit's seqno (`x-pigeonhole-min-primary-seqno`): the replica waits until it has applied
/// it. The primary's background sync is set far off, so a `Buffered` commit reaches the
/// replica only when a later `GroupSync` commit syncs it: a replica held behind for as long
/// as the test likes, with no hook.
#[test]
fn a_read_with_min_primary_seqno_waits_for_the_replica_to_apply_it() {
    let dir = test_dir();
    let db = Pigeonhole::open(
        dir.join("t.phdb"),
        Options::default()
            .create_if_missing(true)
            .shards(1)
            .change_feed(
                pigeonhole::FeedRetention::bytes(64 << 20).sync_interval(Duration::from_secs(600)),
            ),
    )
    .unwrap();
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create()
        .unwrap();
    let p = Server::serve(dir, db, "", limits(), None, |s| s);
    let mut short = limits();
    short.request_timeout = Duration::from_millis(500);
    let r = Server::start_replica_with(test_dir(), &p.url, None, short);
    wait_replica(&r, "caught up", caught_up(&p, None));

    let mut w = put("x", "f", "q", json!({"bytes": b64(b"mine")}));
    w["durability"] = json!("DURABILITY_BUFFERED");
    let (status, body) = p.post("Mutate", &w, None);
    assert_eq!(status, 200, "{body}");
    let seqno = body["commit"]["seqno"].as_str().unwrap().to_owned();
    let get = json!({"table": "t", "row": b64(b"x"), "family": "f", "qualifier": b64(b"q")});
    let header = [(super::replica::MIN_PRIMARY_SEQNO, seqno.as_str())];

    // Not durable on the primary, so not on the replica: the read waits out its deadline,
    // then fails retry-safe, having read nothing.
    let (status, body) = r.rpc_with("Data/Get", &get, None, &header);
    assert_eq!(
        (status, body["code"].as_str()),
        (503, Some("ReplicaBehind")),
        "{body}"
    );
    assert_eq!(body["retry_from"], "RETRY_FROM_REQUEST", "{body}");
    // Without the header, a replica answers what it has.
    let (status, body) = r.post("Get", &get, None);
    assert_eq!((status, body.get("cell").is_none()), (200, true), "{body}");

    // A read that waits, while the primary syncs: a GroupSync commit covers `x`.
    let started = std::time::Instant::now();
    let (status, body) = std::thread::scope(|scope| {
        // Within the replica's 500 ms request timeout.
        let read = scope.spawn(|| r.rpc_with("Data/Get", &get, None, &header));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!read.is_finished(), "the read waits for the replica");
        let sync = put("y", "f", "q", json!({"bytes": b64(b"sync")}));
        assert_eq!(p.post("Mutate", &sync, None).0, 200);
        read.join().unwrap()
    });
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["cell"]["value"],
        b64(b"mine"),
        "it reads its own write"
    );
    assert!(started.elapsed() >= Duration::from_millis(200));

    // Over gRPC, as metadata.
    let mut client = r.grpc();
    let mut req = tonic::Request::new(proto::GetRequest {
        table: "t".into(),
        row: b"x".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert(super::replica::MIN_PRIMARY_SEQNO, seqno.parse().unwrap());
    let got = r.block_on(client.get(req)).unwrap().into_inner();
    assert!(got.cell.is_some());

    // A seqno far ahead: ReplicaBehind over gRPC too (UNAVAILABLE).
    let mut req = tonic::Request::new(proto::GetRequest {
        table: "t".into(),
        row: b"x".to_vec().into(),
        family: "f".into(),
        qualifier: b"q".to_vec().into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        super::replica::MIN_PRIMARY_SEQNO,
        "999999999".parse().unwrap(),
    );
    let err = r.block_on(client.get(req)).unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");

    // Not a number: InvalidArgument. A primary ignores the header. Info doesn't wait.
    let bad = [(super::replica::MIN_PRIMARY_SEQNO, "soon")];
    let (status, body) = r.rpc_with("Data/Get", &get, None, &bad);
    assert_eq!(
        (status, body["code"].as_str()),
        (400, Some("InvalidArgument")),
        "{body}"
    );
    let ahead = [(super::replica::MIN_PRIMARY_SEQNO, "999999999")];
    let (status, body) = p.rpc_with("Data/Get", &get, None, &ahead);
    assert_eq!(status, 200, "a primary ignores it: {body}");
    let (status, body) = r.rpc_with("Admin/Info", &json!({}), None, &ahead);
    assert_eq!(status, 200, "Info answers at once: {body}");
}

#[test]
fn a_read_waits_for_none_and_cross_shard_commits_without_a_later_commit() {
    // cli692's Q1 on #1594: a client's `CommitInfo.seqno` is always one the primary's feed
    // delivers, so a read naming it is answered once the replica applies that commit, with
    // no later commit needed to move the replica past it. A `None` commit and a
    // cross-shard transaction are the cases that could have had no event of their own.
    let p = Server::start_with("", limits(), 4);
    let r = Server::start_replica(test_dir(), &p.url, None);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let read = |seqno: &str| {
        let get = json!({"table": "t", "row": b64(b"row0"), "family": "f", "qualifier": b64(b"q")});
        let header = [(super::replica::MIN_PRIMARY_SEQNO, seqno)];
        let (status, body) = r.rpc_with("Data/Get", &get, None, &header);
        assert_eq!(status, 200, "seqno {seqno}: {body}");
    };
    let mut w = put("a", "f", "q", json!({"bytes": b64(b"a")}));
    w["durability"] = json!("DURABILITY_NONE");
    let (status, body) = p.post("Mutate", &w, None);
    assert_eq!(status, 200, "{body}");
    read(body["commit"]["seqno"].as_str().unwrap());

    let id = begin(&p, None);
    let rows: Vec<Json> = (0..16)
        .flat_map(|i| {
            txn_rows(&format!("row{i}"), b"v")
                .as_array()
                .unwrap()
                .clone()
        })
        .collect();
    let (status, body) = p.rpc(
        "Transactions/Commit",
        &json!({"txn_id": id, "rows": rows}),
        None,
    );
    assert_eq!(status, 200, "{body}");
    read(body["commit"]["seqno"].as_str().unwrap());
}

#[test]
fn a_promotion_releases_waiting_reads_and_a_client_deadline_answers_deadline_exceeded() {
    let p = Server::start("", limits());
    let mut long = limits();
    long.request_timeout = Duration::from_secs(20);
    let r = Server::start_replica_with(test_dir(), &p.url, None, long);
    wait_replica(&r, "caught up", caught_up(&p, None));
    let ahead = || {
        let mut req = tonic::Request::new(proto::GetRequest {
            table: "t".into(),
            row: b"x".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        });
        req.metadata_mut().insert(
            super::replica::MIN_PRIMARY_SEQNO,
            "999999999".parse().unwrap(),
        );
        req
    };

    // The client's deadline is shorter than the server's: it bounds the wait, which ends
    // as the budget wait does, DeadlineExceeded (cli692's R1 on #1594). Called on the
    // service itself: tonic's client ends the call at its own deadline first.
    let service = &r.shared.router.service;
    let get = || {
        ServiceOp::Get(proto::GetRequest {
            table: "t".into(),
            row: b"x".to_vec().into(),
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            ..Default::default()
        })
    };
    let arrived = std::time::Instant::now();
    let deadline = |client: bool| {
        super::replica::ReadDeadline::new(
            arrived,
            Duration::from_secs(20),
            client.then_some(Duration::from_millis(300)),
        )
    };
    let err = r
        .block_on(service.await_primary(&get(), Some(999_999_999), deadline(true)))
        .unwrap_err();
    assert_eq!(
        err.to_status().code(),
        tonic::Code::DeadlineExceeded,
        "{err:?}"
    );
    // One deadline from the request's arrival: time already spent (the budget wait) counts.
    let spent = super::replica::ReadDeadline::new(
        arrived - Duration::from_secs(20),
        Duration::from_secs(20),
        None,
    );
    let err = r
        .block_on(service.await_primary(&get(), Some(999_999_999), spent))
        .unwrap_err();
    assert_eq!(err.to_status().code(), tonic::Code::Unavailable, "{err:?}");
    assert!(arrived.elapsed() < Duration::from_secs(5));

    // A read already waiting when the replica is promoted is answered then, not at its
    // deadline (cli692's R2, simp724's N2).
    let started = std::time::Instant::now();
    let reply = std::thread::scope(|scope| {
        let read = scope.spawn(|| {
            let mut client = r.grpc();
            r.block_on(client.get(ahead()))
        });
        std::thread::sleep(Duration::from_millis(300));
        assert!(!read.is_finished(), "the read waits for the replica");
        let (status, body) = r.rpc("Admin/Promote", &json!({}), None);
        assert_eq!(status, 200, "{body}");
        read.join().unwrap()
    });
    assert!(reply.is_ok(), "{reply:?}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "answered at the promotion, not the 20 s deadline: {:?}",
        started.elapsed()
    );
}

/// A TCP proxy to `upstream` (`host:port`) that forwards each connection's first `budget`
/// bytes from the server, then stalls it: it keeps the connection open and forwards nothing
/// more (#1595: a primary's stream that stops without ending). Connection number `refuse`
/// (from 0), if any, is closed as soon as it's accepted: a reconnect that fails. Stops when
/// `stop` is set.
fn stalling_proxy(
    upstream: &str,
    budget: usize,
    refuse: Option<usize>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let upstream = upstream.to_owned();
    std::thread::spawn(move || {
        let mut accepted = 0;
        while !stop.load(Ordering::Relaxed) {
            let client = match listener.accept() {
                Ok((c, _)) => c,
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
            };
            accepted += 1;
            if refuse == Some(accepted - 1) {
                drop(client);
                continue;
            }
            client.set_nonblocking(false).unwrap();
            let server = std::net::TcpStream::connect(&upstream).unwrap();
            let (mut c_read, mut s_write) =
                (client.try_clone().unwrap(), server.try_clone().unwrap());
            std::thread::spawn(move || {
                let mut buf = [0u8; 16 << 10];
                while let Ok(n) = c_read.read(&mut buf) {
                    if n == 0 || s_write.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            });
            let (mut s_read, mut c_write) = (server, client);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut left = budget;
                let mut buf = [0u8; 16 << 10];
                while left > 0 {
                    let want = left.min(buf.len());
                    match s_read.read(&mut buf[..want]) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if c_write.write_all(&buf[..n]).is_err() {
                                return;
                            }
                            left -= n;
                        }
                    }
                }
                // Stalled: both ends stay open, and nothing more goes through.
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(20));
                }
            });
        }
    });
    url
}

#[test]
fn a_bootstrap_download_that_stalls_resumes_on_a_new_connection() {
    // #1595: a primary's stream that stops sending without ending used to hold a replica in
    // `bootstrapping` for ever, with nothing in Info. Each chunk's wait is now bounded by
    // `--stream-idle-ms`; a stall is a break, recorded in `last_error`, and the download
    // resumes from its offset on a new connection.
    let p = Server::start("", limits());
    // Data that doesn't compress, so the copy spans many 1 MiB chunks.
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in 0..48 {
        let value: Vec<u8> = (0..64 << 10)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let (status, body) = p.post(
            "Mutate",
            &put(&format!("r{i:02}"), "f", "q", json!({"bytes": b64(&value)})),
            None,
        );
        assert_eq!(status, 200, "{body}");
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let upstream = p.url.trim_start_matches("http://").to_owned();
    // Each connection takes one 1 MiB chunk of the copy (and its framing), then stalls: the
    // copy holds the 3 MiB written above, so every bootstrap stalls at least twice, however
    // the copy is laid out (#1615 shrank it from tens of MiB to about its data). The second
    // connection, the first reconnect after a stall, is closed at once: one more break,
    // never a restart from zero (cli692's R2 on #1609).
    let proxy = stalling_proxy(&upstream, 3 << 19, Some(1), Arc::clone(&stop));
    let mut short = limits();
    short.stream_idle = Duration::from_millis(500);
    let r = Server::start_replica_with(test_dir(), &proxy, None, short);
    let mut stalled = false;
    let mut refused = false;
    let mut downloading = false;
    let (mut most, mut restarted) = (0u64, false);
    // Every distinct `last_error` seen, in order, for the failure messages.
    let mut errors: Vec<String> = Vec::new();
    let info = wait_replica(&r, "caught up", |info| {
        let error = info["last_error"].as_str().unwrap_or("");
        if !error.is_empty() && errors.last().is_none_or(|e| e != error) {
            errors.push(error.to_owned());
        }
        stalled |= error.contains("no chunk of the bootstrap copy");
        // The reconnect closed at once is a break of its own, whatever its words: tonic
        // reports a closed connection as "transport error", but one reset (as Windows may
        // do to a socket closed with the request unread) as the OS's error, and a refused
        // connect as "connecting to …". So any break but a stall or a fresh copy counts.
        refused |= error.starts_with("downloading the bootstrap copy")
            && !error.contains("no chunk of the bootstrap copy")
            && !error.contains("starting a fresh copy");
        downloading |= info["bootstrap_phase"] == "downloading";
        if let Some(bytes) = info["bootstrap_bytes"].as_str() {
            let bytes: u64 = bytes.parse().unwrap();
            restarted |= bytes < most;
            most = most.max(bytes);
        }
        caught_up(&p, None)(info)
    });
    stop.store(true, Ordering::Relaxed);
    eprintln!("bootstrap breaks seen: {errors:?}");
    assert!(
        stalled,
        "the stall was recorded as a break: {info}, errors seen {errors:?}"
    );
    assert!(
        refused,
        "the failed reconnect was recorded as a break: {info}, errors seen {errors:?}"
    );
    assert!(
        !restarted,
        "the download resumed, never restarted from zero: errors seen {errors:?}"
    );
    assert!(downloading, "Info showed the download's step");
    assert!(
        info.get("bootstrap_phase").is_none(),
        "no bootstrap step once it's done: {info}"
    );
    assert_eq!(
        get_value(&r, "t", "r47", None),
        get_value(&p, "t", "r47", None)
    );
}

#[test]
fn a_backup_download_answers_at_once_and_heartbeats_while_its_copy_is_written() {
    // #1611: a fresh download used to answer only once the whole copy was written and
    // hashed. Over gRPC it now answers at once with a heartbeat (no data, its id), then one
    // about every second; a resume with the id while the copy is written waits for that same
    // copy. HTTP still waits for the copy, whose size and hash are its headers.
    use sha2::{Digest, Sha256};
    let s = Server::start_slow_copies(Duration::from_millis(2500), false);
    for i in 0..20u32 {
        let value = json!({"bytes": b64(&vec![b'v'; 4096])});
        assert_eq!(
            s.post("Mutate", &put(&format!("r{i:02}"), "f", "q", value), None)
                .0,
            200
        );
    }
    let downloads = s.dir.join("backups").join(".phdb-serve-downloads");
    let bytes = s.block_on(async {
        let mut admin = proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
        let started = std::time::Instant::now();
        let mut stream = admin
            .backup_download(proto::BackupDownloadRequest::default())
            .await
            .unwrap()
            .into_inner();
        let first = tokio::time::timeout(Duration::from_secs(1), stream.message())
            .await
            .expect("an answer before the copy is written")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(first.data.is_empty(), "a heartbeat carries no data");
        assert_eq!(first.offset, 0);
        assert!(!first.backup_id.is_empty());
        let second = tokio::time::timeout(Duration::from_millis(1500), stream.message())
            .await
            .expect("another heartbeat within the second")
            .unwrap()
            .unwrap();
        assert!(second.data.is_empty());
        assert_eq!(second.backup_id, first.backup_id);
        // The client breaks off and resumes with the id while the copy is still written.
        drop(stream);
        let mut stream = admin
            .backup_download(proto::BackupDownloadRequest {
                backup_id: first.backup_id.clone(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let (mut bytes, mut beats, mut last) = (Vec::new(), 0, None);
        while let Some(c) = stream.message().await.unwrap() {
            assert_eq!(c.backup_id, first.backup_id);
            assert_eq!(c.offset, bytes.len() as u64);
            if c.data.is_empty() && c.sha256.is_empty() {
                beats += 1;
            }
            bytes.extend_from_slice(&c.data);
            last = Some(c);
        }
        let last = last.unwrap();
        assert!(beats >= 1, "the resume waited for the copy with heartbeats");
        assert_eq!(last.total, bytes.len() as u64);
        assert_eq!(&last.sha256[..], &Sha256::digest(&bytes)[..]);
        bytes
    });
    assert_eq!(
        std::fs::read_dir(&downloads).unwrap().count(),
        1,
        "the resume read the same copy, never a second"
    );
    let copy = s.dir.join("download.phdb");
    std::fs::write(&copy, &bytes).unwrap();
    assert_eq!(rows_in_backup(&copy).0, 20);
    // Over HTTP the response waits for the copy, then has its size and hash as headers.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let mut resp = agent
        .post(format!("{}/v1/Admin/BackupDownload", s.url))
        .send_json(json!({}))
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let sha = resp
        .headers()
        .get("x-pigeonhole-backup-sha256")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body = resp.body_mut().read_to_vec().unwrap();
    let want: String = Sha256::digest(&body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(sha, want);
    assert_eq!(
        rows_in_backup(&{
            let p = s.dir.join("download-http.phdb");
            std::fs::write(&p, &body).unwrap();
            p
        })
        .0,
        20
    );
}

#[test]
fn a_bootstrap_from_a_primary_whose_copy_outlasts_the_idle_limit_succeeds() {
    // #1611 (cli692's R1 on #1609): a replica waited a fixed 10 minutes for the primary's
    // first answer, so a longer copy could never bootstrap one. With heartbeats, a wait
    // between them is 3 s (the idle limit here is 500 ms), against a copy that takes 5 s,
    // and nothing breaks.
    let p = Server::start_slow_copies(Duration::from_secs(5), false);
    for i in 0..8u32 {
        let (status, body) = p.post(
            "Mutate",
            &put(&format!("r{i}"), "f", "q", json!({"bytes": b64(b"v")})),
            None,
        );
        assert_eq!(status, 200, "{body}");
    }
    let mut short = limits();
    short.stream_idle = Duration::from_millis(500);
    let r = Server::start_replica_with(test_dir(), &p.url, None, short);
    let mut waited = false;
    let info = wait_replica(&r, "caught up", |info| {
        let error = info["last_error"].as_str().unwrap_or("");
        assert!(
            error.is_empty(),
            "a break while the copy was written: {info}"
        );
        waited |= info["bootstrap_phase"] == "waiting for the primary's copy";
        caught_up(&p, None)(info)
    });
    assert!(waited, "Info showed the wait for the copy: {info}");
    assert_eq!(
        get_value(&r, "t", "r7", None),
        get_value(&p, "t", "r7", None)
    );
}

#[test]
fn a_copy_that_fails_after_a_heartbeat_reports_why() {
    // cli692's R1 and R2 on #1628: a copy that fails while its download sends heartbeats
    // must reach the client as its error, not as a stream that ends as if whole (a heartbeat
    // sitting in the channel made the error's `try_send` fail); and a replica must give up
    // with that cause, not with the NotFound of its resumes.
    let s = Server::start_slow_copies(Duration::from_millis(1200), true);
    // Straight through the service, its chunks taken only when the test polls: a heartbeat
    // fills the channel (one chunk) while the copy fails, and the error must still follow.
    let service = &s.shared.router.service;
    let reply = s.block_on(service.call(
        ServiceOp::BackupDownload(proto::BackupDownloadRequest::default()),
        &Caller::anonymous(),
        None,
    ));
    let Ok(super::service::ServiceReply::Download(mut d)) = reply else {
        panic!("a download: {reply:?}");
    };
    std::thread::sleep(Duration::from_millis(1600));
    let mut beats = 0;
    let e = s.block_on(async {
        loop {
            match d.chunks.recv().await {
                Some(Ok(c)) => {
                    assert!(c.data.is_empty(), "no data from a failed copy");
                    beats += 1;
                    // The task's next heartbeat refills the channel, and the copy's failure
                    // comes after it: the error must wait for room, not be dropped.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Some(Err(e)) => break e,
                None => panic!("the download ended as if whole after {beats} heartbeats"),
            }
        }
    });
    assert!(beats >= 1);
    assert!(e.message.contains(REFUSED), "{e:?}");
    s.block_on(async {
        let mut admin = proto::admin_client::AdminClient::new(
            tonic::transport::Channel::from_shared(s.url.clone())
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
        let mut stream = admin
            .backup_download(proto::BackupDownloadRequest::default())
            .await
            .unwrap()
            .into_inner();
        // Take no chunk for a while, so a heartbeat sits in the channel when the copy fails.
        tokio::time::sleep(Duration::from_millis(1600)).await;
        let mut beats = 0;
        let e = loop {
            match stream.message().await {
                Ok(Some(c)) => {
                    assert!(c.data.is_empty(), "no data from a failed copy");
                    beats += 1;
                }
                Ok(None) => panic!("the stream ended as if whole after {beats} heartbeats"),
                Err(e) => break e,
            }
        };
        assert!(beats >= 1);
        assert!(e.message().contains(REFUSED), "{e:?}");
    });
    // Over HTTP: the copy's error, not "ended early".
    let (status, e) = s.rpc("Admin/BackupDownload", &json!({}), None);
    assert_ne!(status, 200);
    assert!(
        e["message"].as_str().unwrap_or("").contains(REFUSED),
        "{status}: {e}"
    );
    // A replica's bootstrap gives up with the cause, after starting fresh copies.
    let r = Server::start_replica(test_dir(), &s.url, None);
    let info = wait_replica(&r, "a failed bootstrap", |info| info["state"] == "retrying");
    let error = info["last_error"].as_str().unwrap_or("");
    assert!(error.contains(REFUSED), "{info}");
    assert!(!error.contains("no such backup download"), "{info}");
}
