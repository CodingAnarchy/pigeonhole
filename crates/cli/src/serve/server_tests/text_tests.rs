use super::*;

/// `texts::STEP_DELAY_MS` and `texts::HOLD_PACK` are process-wide: a test that slows
/// rebuilds or holds packs holds this from setting one until it resets it, so another test's
/// reset can't speed its rebuild past its fence or close (#1995).
static STEP_DELAY: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Rebuilds wait 5 ms after each document until this is dropped, or packs hold after each
/// chunk ([`hold_packs`]).
struct SlowSteps {
    _held: std::sync::MutexGuard<'static, ()>,
}

fn slow_steps() -> SlowSteps {
    let held = STEP_DELAY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    crate::serve::texts::STEP_DELAY_MS.store(5, Ordering::Relaxed);
    SlowSteps { _held: held }
}

/// Until this is dropped, a pack of `db` waits after each chunk until a client or a resume
/// wants the slot (`texts::HOLD_PACK`). Rebuilds aren't slowed, nor other servers' packs.
fn hold_packs(db: &Pigeonhole) -> SlowSteps {
    let held = STEP_DELAY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *crate::serve::texts::HOLD_PACK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(db.path().to_path_buf());
    SlowSteps { _held: held }
}

impl Drop for SlowSteps {
    // Before the lock is released: the field drops after this.
    fn drop(&mut self) {
        crate::serve::texts::STEP_DELAY_MS.store(0, Ordering::Relaxed);
        *crate::serve::texts::HOLD_PACK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

// ---- Text (D313 ticket 7c, D309, ICR 0105) ----

fn text_hits(r: &Json) -> Vec<String> {
    r["hits"]
        .as_array()
        .map(|h| {
            h.iter()
                .map(|h| h["id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn a_text_index_is_created_written_searched_rebuilt_and_verified() {
    let s = Server::start("", limits());
    let spec = json!({"name": "tx", "source_table": "t", "source_family": "f",
                      "source_qualifier": b64(b"body"), "stop_words": true});
    let (status, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(status, 200, "{e}");
    let (_, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(e["code"].as_str(), Some("TableExists"), "{e}");
    let mut again = spec.clone();
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Text/CreateIndex", &again, None).0, 200);
    for (id, text) in [
        ("doc:1", "We chose Postgres in the March meeting"),
        ("doc:2", "The meeting moved to April"),
        ("note:3", "State of the Art"),
    ] {
        let (status, e) = s.rpc(
            "Text/Put",
            &json!({"index": "tx", "id": b64(id.as_bytes()), "text": text}),
            None,
        );
        assert_eq!(status, 200, "{id}: {e}");
    }
    // The source column holds the text: Put writes through the index.
    let (_, r) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"doc:2"), "family": "f",
                                       "qualifier": b64(b"body")}),
        None,
    );
    assert!(r["cell"].is_object(), "{r}");
    // The syntax.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "postgres \"march meeting\" meeting", "k": 10}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    // Clauses, by field name: `term` as given (so "March" misses a lowercased index), `text`
    // and a phrase's text tokenised (so "March Meeting" matches), with highlights whose
    // clause indexes are the request's.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "k": 10, "highlight": true, "clauses": {"clauses": [
            {"term": "March"},
            {"phrase": {"text": "March Meeting"}},
            {"fuzzy": {"term": "postgress", "fuzziness": "FUZZINESS_AUTO"}},
            {"text": "April"},
        ]}}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    let top = &r["hits"][0];
    assert_eq!(
        top["text"].as_str(),
        Some("We chose Postgres in the March meeting")
    );
    let clauses: Vec<u64> = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["clause"].as_u64().unwrap_or(0))
        .collect();
    assert!(
        clauses.contains(&1) && clauses.contains(&2) && !clauses.contains(&0),
        "{top}"
    );
    let phrase = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["clause"].as_u64() == Some(1))
        .unwrap();
    let (start, end) = (
        phrase["start"].as_str().unwrap().parse::<usize>().unwrap(),
        phrase["end"].as_str().unwrap().parse::<usize>().unwrap(),
    );
    assert_eq!(
        &"We chose Postgres in the March meeting"[start..end],
        "March meeting"
    );
    // A phrase of only stop words, or of nothing, matches nothing.
    for text in ["the", ""] {
        let (status, r) = s.rpc(
            "Text/Search",
            &json!({"index": "tx", "k": 10,
                    "clauses": {"clauses": [{"phrase": {"text": text}}]}}),
            None,
        );
        assert_eq!(status, 200, "{text:?}: {r}");
        assert!(text_hits(&r).is_empty(), "{text:?}: {r}");
    }
    // Only ids under a prefix.
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "state meeting", "k": 10, "id_prefix": b64(b"note:")}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"note:3")]);
    // Refused: a repeated clause (it would shift the clauses' indexes), k out of range, a
    // bad query, a slop past 65,535, no query, an index that doesn't exist.
    for (req, code) in [
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"term": "a"}, {"text": "b"}, {"term": "a"}]}}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 0, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 10_001, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "syntax": "\"unclosed"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"phrase": {"text": "a b", "slop": 70_000}}]}}),
            "InvalidArgument",
        ),
        (json!({"index": "tx", "k": 1}), "InvalidArgument"),
        (
            json!({"index": "nope", "k": 1, "syntax": "a"}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Text/Search", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
    // A delete, then the index's own check and a rebuild that changes nothing.
    assert_eq!(
        s.rpc(
            "Text/Delete",
            &json!({"index": "tx", "id": b64(b"doc:2")}),
            None
        )
        .0,
        200
    );
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    let (status, v) = s.rpc("Text/Verify", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{v}");
    assert_eq!(v["documents"], json!("2"), "{v}");
    assert!(v.get("forward_mismatch").is_none_or(|m| m == "0"), "{v}");
    assert!(v.get("source_mismatch").is_none_or(|m| m == "0"), "{v}");
    let (status, r) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{r}");
    assert_eq!(r["documents"], json!("2"), "{r}");
    assert!(r.get("changed").is_none_or(|c| c == "0"), "{r}");
    // A catalog change on a replica drops the text handles with the rest (#1436); the next
    // call opens the index again by name.
    let service = &s.shared.router.service;
    assert!(service.part().unwrap().texts.cached("tx").is_some());
    service.tables_changed();
    assert!(service.part().unwrap().texts.cached("tx").is_none());
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    // Shutdown's close waits for a rebuild, and refuses one after it (ai1249's S1).
    let texts = &s.shared.router.service.part().unwrap().texts;
    texts.close();
    let (_, e) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(e["code"].as_str(), Some("Closed"), "{e}");
    // Over gRPC.
    let hits = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::text_client::TextClient::new(channel)
            .search(proto::TextSearchRequest {
                index: "tx".into(),
                k: 5,
                query: Some(proto::text_search_request::Query::Syntax("postgres".into())),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .hits
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(&hits[0].id[..], b"doc:1");
}

/// #1916 (the coordinator's option (a)): a table a text index reads can't be dropped
/// while the index is there. The refusal names every such index; once they're dropped, the
/// table drops. Another table, and the index's own table, drop as before.
#[test]
fn a_text_index_s_source_table_is_refused_a_drop_naming_the_index() {
    let s = Server::start("", limits());
    for name in ["tx", "ty"] {
        let spec = json!({"name": name, "source_table": "t", "source_family": "f",
                          "source_qualifier": b64(b"body")});
        assert_eq!(s.rpc("Text/CreateIndex", &spec, None).0, 200);
    }
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "t"}), None);
    assert_eq!(
        (status, e["code"].as_str()),
        (409, Some("FailedPrecondition")),
        "{e}"
    );
    let message = e["message"].as_str().unwrap_or("");
    assert!(
        message.contains("\"tx\"") && message.contains("\"ty\""),
        "{e}"
    );
    // Nothing was dropped: the index still writes through to its source.
    let (status, e) = s.rpc(
        "Text/Put",
        &json!({"index": "tx", "id": b64(b"doc"), "text": "still here"}),
        None,
    );
    assert_eq!(status, 200, "{e}");
    for name in ["tx", "ty"] {
        let (status, e) = s.rpc("Schema/DropTable", &json!({"name": name}), None);
        assert_eq!(status, 200, "{name}: {e}");
    }
    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "t"}), None);
    assert_eq!(status, 200, "{e}");
}

/// #1916: a drop of a table that a `Text.CreateIndex` in flight reads is refused too, so the
/// create can't finish over a dropped (or re-made) source.
#[test]
fn a_drop_of_a_source_table_while_an_index_over_it_is_created_is_refused() {
    let s = Server::start("", limits());
    let reached = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    crate::serve::texts::HOLD_CREATE.lock().unwrap().push((
        "tx_1916_held".into(),
        Arc::clone(&reached),
        Arc::clone(&release),
    ));
    std::thread::scope(|scope| {
        let creating = scope.spawn(|| {
            let spec = json!({"name": "tx_1916_held", "source_table": "t",
                              "source_family": "f", "source_qualifier": b64(b"body")});
            s.rpc("Text/CreateIndex", &spec, None)
        });
        reached.wait();
        let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "t"}), None);
        release.wait();
        crate::serve::texts::HOLD_CREATE
            .lock()
            .unwrap()
            .retain(|(n, _, _)| n != "tx_1916_held");
        assert_eq!(
            (status, e["code"].as_str()),
            (409, Some("FailedPrecondition")),
            "{e}"
        );
        assert!(
            e["message"]
                .as_str()
                .unwrap_or("")
                .contains("being created"),
            "{e}"
        );
        let (status, e) = creating.join().unwrap();
        assert_eq!(status, 200, "{e}");
    });
    // Built, it's named by the refusal as any index is.
    let (_, e) = s.rpc("Schema/DropTable", &json!({"name": "t"}), None);
    assert!(
        e["message"].as_str().unwrap_or("").contains("tx_1916_held"),
        "{e}"
    );
}

/// #1588: dropping a text index's table evicts its cached handle. A search then answers
/// `TableNotFound`, and a new index made under the name (with `if_missing`, which a cached
/// handle used to answer at once) is the one served.
#[test]
fn dropping_a_text_index_s_table_evicts_its_handle() {
    let s = Server::start("", limits());
    let spec = json!({"name": "tx", "source_table": "t", "source_family": "f",
                      "source_qualifier": b64(b"body")});
    let put = |id: &[u8], text: &str| {
        let (status, e) = s.rpc(
            "Text/Put",
            &json!({"index": "tx", "id": b64(id), "text": text}),
            None,
        );
        assert_eq!(status, 200, "{e}");
    };
    let search = || {
        s.rpc(
            "Text/Search",
            &json!({"index": "tx", "syntax": "meeting", "k": 10}),
            None,
        )
    };
    assert_eq!(s.rpc("Text/CreateIndex", &spec, None).0, 200);
    put(b"old", "the old meeting");
    let (status, r) = search();
    assert_eq!((status, text_hits(&r)), (200, vec![b64(b"old")]), "{r}");

    let (status, e) = s.rpc("Schema/DropTable", &json!({"name": "tx"}), None);
    assert_eq!(status, 200, "{e}");
    let (status, r) = search();
    assert_eq!(
        (status, r["code"].as_str()),
        (404, Some("TableNotFound")),
        "{r}"
    );

    // Made again over a new source column: `if_missing` opens the new index.
    let mut again = spec.clone();
    again["if_missing"] = json!(true);
    again["source_qualifier"] = json!(b64(b"body2"));
    assert_eq!(s.rpc("Text/CreateIndex", &again, None).0, 200);
    put(b"new", "the new meeting");
    let (status, r) = search();
    assert_eq!((status, text_hits(&r)), (200, vec![b64(b"new")]), "{r}");
}

// ---- A rebuild a fence stops, and its resume (#1756) ----

/// The database's source table `t` (family `f`, column `body`) with `n` documents, and the
/// text index `tx` over it (its backfill indexes them, so a rebuild visits each and changes
/// nothing).
fn indexed(db: &Pigeonhole, n: usize) {
    let table = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..n {
        table
            .mutate(format!("doc:{i:04}").as_bytes())
            .put("f", b"body", b"meeting notes")
            .commit()
            .unwrap();
    }
    drop(
        pigeonhole_text::Index::create(
            db,
            "tx",
            pigeonhole_text::Source::new(&table, "f", b"body"),
            pigeonhole_text::IndexOptions::new(),
        )
        .unwrap(),
    );
}

/// A partition service on `s`'s database behind `gate`, as a cluster node's partition is
/// (its own texts, the node's rebuild slot), resuming a pending rebuild as an open does.
fn gated_part(s: &Server, gate: &Arc<crate::serve::gate::WriteGate>) -> Service {
    let service = &s.shared.router.service;
    let part = service.for_partition(
        service.part().unwrap().db.clone(),
        "p1",
        Arc::clone(gate),
        PartitionConfig::new(crate::serve::backup::Backups::new(None, 1).unwrap()),
    );
    // As a node's open does.
    part.resume_text_rebuilds();
    part
}

/// Waits (30 s at most) until no rebuild of `tx` is pending.
fn resumed(db: &Pigeonhole) {
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while !pigeonhole_text::Index::pending_rebuilds(db)
        .unwrap()
        .is_empty()
    {
        assert!(
            std::time::Instant::now() < until,
            "the stopped rebuild never resumed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let v = pigeonhole_text::Index::open(db, "tx")
        .unwrap()
        .verify()
        .unwrap();
    assert!(v.is_consistent(), "{v:?}");
}

#[test]
fn a_fence_stops_a_text_rebuild_at_its_next_document_and_it_resumes_once_unfenced() {
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    let gate = crate::serve::gate::WriteGate::opened("p1", false, Default::default());
    gate.renew(u64::MAX);
    let part = gated_part(&s, &gate);
    let slow = slow_steps();
    // The fence lands mid-rebuild (400 documents at 5 ms each).
    let fenced_at = {
        let gate = Arc::clone(&gate);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            gate.fence();
            std::time::Instant::now()
        })
    };
    let e = s
        .block_on(part.call(
            ServiceOp::TextRebuild(proto::TextIndexRequest { index: "tx".into() }),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    let answered = std::time::Instant::now();
    let fenced_at = fenced_at.join().unwrap();
    drop(slow);
    // Within a document or two of the fence, not the whole rebuild (or the fence's drain).
    assert!(
        answered.saturating_duration_since(fenced_at) < Duration::from_secs(1),
        "{:?} after the fence",
        answered.saturating_duration_since(fenced_at)
    );
    assert_eq!(e.code, "Fenced", "{e:?}");
    assert!(e.message.contains("stopped after"), "{e:?}");
    assert!(e.message.contains("resumes on its own"), "{e:?}");
    assert_eq!(
        pigeonhole_text::Index::pending_rebuilds(&db).unwrap(),
        ["tx"],
        "the cursor stays"
    );
    // Still fenced: nothing resumes (a resume would write past the fence).
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        pigeonhole_text::Index::pending_rebuilds(&db).unwrap(),
        ["tx"]
    );
    gate.unfence();
    resumed(&db);
    part.part().unwrap().close_users();
}

#[test]
fn a_close_stops_a_text_rebuild_and_the_next_open_resumes_it() {
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    let gate = crate::serve::gate::WriteGate::opened("p1", false, Default::default());
    gate.renew(u64::MAX);
    let part = gated_part(&s, &gate);
    let slow = slow_steps();
    let texts = Arc::clone(&part.part().unwrap().texts);
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        let at = std::time::Instant::now();
        texts.close();
        at.elapsed()
    });
    let e = s
        .block_on(part.call(
            ServiceOp::TextRebuild(proto::TextIndexRequest { index: "tx".into() }),
            &Caller::anonymous(),
            None,
        ))
        .unwrap_err();
    let took = closer.join().unwrap();
    drop(slow);
    // The close no longer waits for the whole rebuild.
    assert!(took < Duration::from_secs(1), "the close took {took:?}");
    // A cluster partition's close answers `PartitionUnavailable` (D317 ticket 6), carrying
    // what the rebuild did.
    assert_eq!(e.code, "PartitionUnavailable", "{e:?}");
    assert!(e.message.contains("stopped after"), "{e:?}");
    assert_eq!(
        pigeonhole_text::Index::pending_rebuilds(&db).unwrap(),
        ["tx"]
    );
    // The partition opened again (a restart, or a move's open): its pending rebuild resumes.
    let again = gated_part(&s, &gate);
    resumed(&db);
    again.part().unwrap().close_users();
}

/// Leaves a rebuild of index `name` (over `t`) stopped at once: an empty cursor, pending.
fn stopped(db: &Pigeonhole, name: &str) {
    let index = pigeonhole_text::Index::open(db, name).unwrap();
    let r = index
        .rebuild_with(|_| std::ops::ControlFlow::Break(()))
        .unwrap();
    assert!(r.stopped);
}

#[test]
fn a_pending_rebuild_that_cant_resume_doesnt_hold_back_the_others() {
    // ai1249's must on #1975: a pending index whose resume fails (here, a repair a crash left:
    // `Busy`) is passed over by the run, and the next one still resumes.
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 50);
    let t = db.table("t").unwrap().open().unwrap();
    for name in ["ta", "tb"] {
        drop(
            pigeonhole_text::Index::create(
                &db,
                name,
                pigeonhole_text::Source::new(&t, "f", b"body"),
                pigeonhole_text::IndexOptions::new(),
            )
            .unwrap(),
        );
        stopped(&db, name);
    }
    // `ta` (first by name) has its repair flag set, as a crashed repair leaves it.
    db.table("ta")
        .unwrap()
        .open()
        .unwrap()
        .mutate(b"G")
        .put("meta", b"repair", b"")
        .commit()
        .unwrap();
    let gate = crate::serve::gate::WriteGate::opened("p1", false, Default::default());
    gate.renew(u64::MAX);
    let part = gated_part(&s, &gate);
    let until = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let pending = pigeonhole_text::Index::pending_rebuilds(&db).unwrap();
        if pending == ["ta"] {
            break;
        }
        assert!(
            std::time::Instant::now() < until,
            "tb never resumed: {pending:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    part.part().unwrap().close_users();
}

#[test]
fn a_clients_rebuild_takes_the_slot_from_a_resume_and_starts_over() {
    // ai1249's should 2 on #1975: a client's `Text.Rebuild` while a resume holds the node's
    // rebuild slot waits for the resume to stop at its next document, then rebuilds from the
    // top (all 400 documents), instead of `Overloaded`.
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    stopped(&db, "tx");
    let slow = slow_steps();
    let gate = crate::serve::gate::WriteGate::opened("p1", false, Default::default());
    gate.renew(u64::MAX);
    let part = gated_part(&s, &gate);
    // The resume is under way (400 documents at 5 ms each).
    std::thread::sleep(Duration::from_millis(300));
    let yielded = crate::serve::texts::YIELDED.load(Ordering::SeqCst);
    let r = s.block_on(part.call(
        ServiceOp::TextRebuild(proto::TextIndexRequest { index: "tx".into() }),
        &Caller::anonymous(),
        None,
    ));
    drop(slow);
    let Ok(service::ServiceReply::TextRebuild(r)) = r else {
        panic!("{r:?}");
    };
    assert_eq!(r.documents, 400, "{r:?}");
    // The resume stopped for it, rather than finishing first (ai1249's S1 on #1996: a time
    // bound barely told the two apart). Only this test makes a resume yield.
    assert!(
        crate::serve::texts::YIELDED.load(Ordering::SeqCst) > yielded,
        "the resume finished instead of yielding the slot"
    );
    assert!(
        pigeonhole_text::Index::pending_rebuilds(&db)
            .unwrap()
            .is_empty()
    );
    part.part().unwrap().close_users();
}

// ---- `--text-pack`: the background pack (#1585, ICR 0143) ----

/// `tx`'s check: its postings in blocks, and that it is consistent.
fn packed_postings(db: &Pigeonhole) -> u64 {
    let v = pigeonhole_text::Index::open(db, "tx")
        .unwrap()
        .verify()
        .unwrap();
    assert!(v.is_consistent(), "{v:?}");
    v.packed
}

/// Waits (30 s at most) until `tx` has `want` postings in blocks.
fn packed(db: &Pigeonhole, want: u64) {
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while packed_postings(db) != want {
        assert!(
            std::time::Instant::now() < until,
            "the index was never packed: {} of {want} postings in blocks",
            packed_postings(db)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The top 10 of a search of `tx` for "meeting".
fn top_meeting(db: &Pigeonhole) -> Vec<(Vec<u8>, u64)> {
    pigeonhole_text::Index::open(db, "tx")
        .unwrap()
        .search(pigeonhole_text::Query::new().term("meeting"), 10)
        .run()
        .unwrap()
        .into_iter()
        .map(|h| (h.id, h.score.to_bits()))
        .collect()
}

fn metrics_text(s: &Server) -> String {
    let (_, m) = s.rpc("Admin/Metrics", &json!({}), None);
    m["text"].as_str().unwrap().to_owned()
}

#[test]
fn a_server_without_text_pack_never_packs_an_index() {
    let s = Server::start("", limits());
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    // As the server's start does, and a promote: neither starts a pack.
    s.shared.router.service.resume_text_rebuilds();
    s.shared.router.service.resume_text_rebuilds();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(packed_postings(&db), 0, "packed without --text-pack");
    let text = metrics_text(&s);
    assert!(!text.contains("pigeonhole_serve_text_pack"), "{text}");
}

#[test]
fn text_pack_packs_the_indexes_in_the_background_and_searches_agree() {
    let s = Server::start_custom("", limits(), 1, |s| s.text_pack(true));
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    let before = top_meeting(&db);
    assert_eq!(before.len(), 10);
    // The pack thread starts with the server's start; a later call makes it pass at once.
    s.shared.router.service.resume_text_rebuilds();
    s.shared.router.service.resume_text_rebuilds();
    // Both terms' 400 postings.
    packed(&db, 800);
    assert_eq!(top_meeting(&db), before);
    // A document written since is a cell, found beside the blocks, and packed by the next
    // pass.
    let (status, r) = s.rpc(
        "Text/Put",
        &json!({"index": "tx", "id": b64(b"doc:9999"), "text": "meeting meeting meeting"}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    let after = top_meeting(&db);
    assert_eq!(after[0].0, b"doc:9999", "{after:?}");
    // The counter moves when the pack's call returns, a moment after its last chunk's
    // commit is visible (a Windows CI run read it in between).
    let until = std::time::Instant::now() + Duration::from_secs(30);
    let text = loop {
        let text = metrics_text(&s);
        if text.contains("pigeonhole_serve_text_pack_postings_total 800") {
            break text;
        }
        assert!(std::time::Instant::now() < until, "{text}");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(
        text.contains("pigeonhole_serve_text_pack_refused_total 0"),
        "{text}"
    );
    assert!(
        text.contains("# TYPE pigeonhole_serve_text_pack_passes_total counter"),
        "{text}"
    );
    assert!(
        text.contains("pigeonhole_serve_text_pack_conflicts_total 0"),
        "{text}"
    );
}

#[test]
fn text_pack_leaves_an_index_alone_while_a_reader_process_is_attached() {
    let s = Server::start_custom("", limits(), 1, |s| s.text_pack(true));
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    // A reader that may be a 1.2 build: the first pack is refused while it is attached.
    let reader =
        Pigeonhole::open_reader(s.dir.join("t.phdb"), pigeonhole::ReaderOptions::default())
            .unwrap();
    s.shared.router.service.resume_text_rebuilds();
    s.shared.router.service.resume_text_rebuilds();
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while metrics_text(&s).contains("pigeonhole_serve_text_pack_refused_total 0") {
        assert!(
            std::time::Instant::now() < until,
            "the pack was never refused"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(packed_postings(&db), 0, "packed under a reader");
    // Counted once for the index, however many passes find it refused (ai1249 on #2018).
    s.shared.router.service.resume_text_rebuilds();
    std::thread::sleep(Duration::from_millis(500));
    let text = metrics_text(&s);
    assert!(
        text.contains("pigeonhole_serve_text_pack_refused_total 1"),
        "{text}"
    );
    assert_eq!(packed_postings(&db), 0, "packed under a reader");
    drop(reader);
    s.shared.router.service.resume_text_rebuilds();
    packed(&db, 800);
}

#[test]
fn text_pack_waits_out_a_fence() {
    let s = Server::start_custom("", limits(), 1, |s| s.text_pack(true));
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    let gate = crate::serve::gate::WriteGate::opened("p1", false, Default::default());
    gate.renew(u64::MAX);
    gate.fence();
    // The partition's pack thread starts here, fenced: it writes nothing.
    let part = gated_part(&s, &gate);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(packed_postings(&db), 0, "packed past a fence");
    gate.unfence();
    packed(&db, 800);
    part.part().unwrap().close_users();
}

/// A server with `--text-pack` whose pack of `tx` (400 documents, two chunks) has done its
/// first chunk and holds in its step before the second ([`hold_packs`]) until a client or
/// a resume wants the node's slot, which the pack holds. A fixed wait there let the pack
/// finish first on a slow runner (#2031's Windows job). Rebuilds aren't slowed.
///
/// Also the counts of packs that had yielded, to a client and to a resume, when this took
/// the steps' lock: hold the `SlowSteps` until the pack's yield is checked against them.
/// The two tests that make a pack yield both come through here, and each checks its own
/// reason's count, so one's yield can't pass for the other's (cli692 on #2018).
fn packing() -> (Server, Pigeonhole, SlowSteps, (usize, usize)) {
    let s = Server::start_custom("", limits(), 1, |s| s.text_pack(true));
    let db = s.shared.router.service.part().unwrap().db.clone();
    indexed(&db, 400);
    let slow = hold_packs(&db);
    let yielded = (
        crate::serve::texts::PACK_YIELDED_TO_CLIENT.load(Ordering::SeqCst),
        crate::serve::texts::PACK_YIELDED_TO_RESUME.load(Ordering::SeqCst),
    );
    s.shared.router.service.resume_text_rebuilds();
    s.shared.router.service.resume_text_rebuilds();
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while packed_postings(&db) == 0 {
        assert!(std::time::Instant::now() < until, "the pack never started");
        std::thread::sleep(Duration::from_millis(10));
    }
    // The pack holds after that chunk until the test's client or resume wants the slot.
    assert!(packed_postings(&db) < 800, "the pack finished in one chunk");
    (s, db, slow, yielded)
}

#[test]
fn a_clients_text_rebuild_takes_the_slot_from_a_pack() {
    let (s, db, slow, yielded) = packing();
    // It waits for the pack to stop at its next chunk, as it does for a resume, rather than
    // being refused.
    let r = s.block_on(s.shared.router.service.call(
        ServiceOp::TextRebuild(proto::TextIndexRequest { index: "tx".into() }),
        &Caller::anonymous(),
        None,
    ));
    let Ok(service::ServiceReply::TextRebuild(r)) = r else {
        panic!("{r:?}");
    };
    assert_eq!(r.documents, 400, "{r:?}");
    // The pack stopped for it, rather than finishing first. Only this test and the next make
    // a pack yield, one at a time, each counting from where it took the lock (`packing`).
    assert!(
        crate::serve::texts::PACK_YIELDED_TO_CLIENT.load(Ordering::SeqCst) > yielded.0,
        "the pack finished instead of yielding the slot"
    );
    drop(slow);
    // The next pass packs the rest.
    s.shared.router.service.resume_text_rebuilds();
    packed(&db, 800);
}

/// cli692's S1 on #2018: a stopped rebuild's resume doesn't wait behind a pack.
#[test]
fn a_stopped_rebuilds_resume_takes_the_slot_from_a_pack() {
    let (s, db, slow, yielded) = packing();
    // A second index over the same source with a rebuild pending from the top, as a
    // cluster's create leaves one.
    let table = db.table("t").unwrap().open().unwrap();
    drop(
        pigeonhole_text::Index::create_unfilled(
            &db,
            "ty",
            pigeonhole_text::Source::new(&table, "f", b"body"),
            pigeonhole_text::IndexOptions::new(),
        )
        .unwrap(),
    );
    assert_eq!(
        pigeonhole_text::Index::pending_rebuilds(&db).unwrap(),
        ["ty"]
    );
    // Its resume finds the pack on the slot, and the pack gives way at its next chunk.
    s.shared.router.service.resume_text_rebuilds();
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while !pigeonhole_text::Index::pending_rebuilds(&db)
        .unwrap()
        .is_empty()
    {
        assert!(
            std::time::Instant::now() < until,
            "the stopped rebuild never resumed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        crate::serve::texts::PACK_YIELDED_TO_RESUME.load(Ordering::SeqCst) > yielded.1,
        "the resume waited for the pack to finish"
    );
    drop(slow);
    // The pack carries on once the resume is done, and packs the new index too.
    packed(&db, 800);
    let v = pigeonhole_text::Index::open(&db, "ty")
        .unwrap()
        .verify()
        .unwrap();
    assert!(v.is_consistent(), "{v:?}");
    assert_eq!(v.documents, 400, "{v:?}");
}

/// cli692's S2 on #2018: a `--replica-of` replica started with `--text-pack` never packs
/// (its writes are refused); once promoted it does.
#[test]
fn a_replica_with_text_pack_packs_only_once_promoted() {
    let p = Server::start("", limits());
    indexed(&p.shared.router.service.part().unwrap().db.clone(), 400);
    let r = Server::start_replica_custom(
        test_dir(),
        &p.url,
        None,
        limits(),
        Options::default(),
        |s| s.text_pack(true),
    );
    wait_replica(&r, "caught up", caught_up(&p, None));
    let db = r.shared.router.service.part().unwrap().db.clone();
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(packed_postings(&db), 0, "a replica packed");
    let text = metrics_text(&r);
    assert!(
        text.contains("pigeonhole_serve_text_pack_passes_total 0"),
        "{text}"
    );
    let (status, body) = r.rpc("Admin/Promote", &json!({}), None);
    assert_eq!(status, 200, "{body}");
    packed(&db, 800);
}
