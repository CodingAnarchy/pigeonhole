//! D329 engine PR 1 (ICR 0140): transactions prepared for an external coordinator. A
//! prepare logs and holds the shares, and its decision waits for `Engine::decide`: a commit
//! applies them, an abort drops them, and a second decide answers `None`.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, EngineOptions, Error, FamilyOptions, Fault, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 4 << 20);
    o.pin_threads = false;
    o
}

fn get_bytes(db: &Engine, t: &pigeonhole_engine::TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    let f = t.family("f").unwrap().id;
    db.get(&snap, t.id, f, row, b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

/// A transaction that reads `read` and writes `value` to each of `rows`.
fn txn(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    read: &[u8],
    rows: &[&[u8]],
    value: &[u8],
) -> pigeonhole_engine::Txn {
    let f = t.family("f").unwrap().id;
    let mut txn = db.begin().unwrap();
    let _ = txn.get(t.id, f, read, b"q").unwrap();
    for row in rows {
        txn.batch()
            .put(t.id, f, row, b"q", None, ValueRef::Bytes(value))
            .unwrap();
    }
    txn
}

#[test]
fn a_prepared_transaction_waits_for_its_decision_then_commits_or_aborts() {
    // Two shards, rows spread over both: the shares are on each.
    let vfs = SimVfs::new(21);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let rows: Vec<Vec<u8>> = (0..16).map(|i| format!("row{i:02}").into_bytes()).collect();
    let rows: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();

    // Prepared: nothing visible, the xid in doubt.
    let p = txn(&db, &t, b"row00", &rows, b"one")
        .prepare(b"x1", Durability::GroupSync)
        .unwrap();
    assert!(p.seqno > 0);
    assert!(rows.iter().all(|r| get_bytes(&db, &t, r).is_none()));
    let doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
    assert_eq!(doubt, [b"x1".to_vec()]);
    // The same xid again: refused while it's in doubt.
    assert!(matches!(
        txn(&db, &t, b"row00", &rows[..1], b"dup").prepare(b"x1", Durability::GroupSync),
        Err(Error::InvalidArgument(_))
    ));

    // Committed: every share applied, at a seqno above the id.
    let c = db.decide(b"x1", true).unwrap().expect("in doubt");
    assert!(c.seqno > p.seqno);
    assert!(
        rows.iter()
            .all(|r| get_bytes(&db, &t, r).as_deref() == Some(&b"one"[..]))
    );
    assert!(db.in_doubt().is_empty());
    // Decided already: `None`, idempotent.
    assert_eq!(db.decide(b"x1", true).unwrap(), None);
    assert_eq!(db.decide(b"x1", false).unwrap(), None);

    // Aborted: nothing applied, the rows as they were.
    txn(&db, &t, b"row00", &rows, b"two")
        .prepare(b"x2", Durability::GroupSync)
        .unwrap();
    // The abort takes a seqno of its own but is no write: no commit is counted (perf2's E1,
    // condition 3 on ICR 0140), and nothing reaches a memtable (the reads below).
    let commits = |db: &Engine| db.metrics().commits.iter().sum::<u64>();
    let before = commits(&db);
    let a = db.decide(b"x2", false).unwrap().expect("in doubt");
    assert!(a.seqno > c.seqno, "the abort's own seqno");
    assert_eq!(commits(&db), before, "an abort counts no commit");
    assert!(
        rows.iter()
            .all(|r| get_bytes(&db, &t, r).as_deref() == Some(&b"one"[..]))
    );
    assert!(db.in_doubt().is_empty());

    // A plain commit after both: its rows are free (nothing held), and it's visible.
    let mut wb = WriteBatch::new();
    let f = t.family("f").unwrap().id;
    wb.put(t.id, f, b"row00", b"q", None, ValueRef::Bytes(b"three"))
        .unwrap();
    db.commit(wb, Some(Durability::GroupSync)).unwrap();
    assert_eq!(get_bytes(&db, &t, b"row00").as_deref(), Some(&b"three"[..]));
    db.close().unwrap();
}

#[test]
fn a_prepare_that_read_a_row_since_written_conflicts() {
    let vfs = SimVfs::new(22);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let x = txn(&db, &t, b"acct", &[b"other"], b"v");
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"acct", b"q", None, ValueRef::Bytes(b"changed"))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert!(matches!(
        x.prepare(b"x3", Durability::GroupSync),
        Err(Error::Conflict)
    ));
    assert!(db.in_doubt().is_empty());
    // The refused prepare left no reservation: the xid prepares again (a leaked one would
    // answer `InvalidArgument`), and `decide` finds it.
    txn(&db, &t, b"acct", &[b"other"], b"v")
        .prepare(b"x3", Durability::GroupSync)
        .unwrap();
    db.decide(b"x3", false).unwrap().expect("in doubt");
    db.close().unwrap();
}

#[test]
fn a_share_in_doubt_survives_a_close_and_a_reopen_and_is_decided_after() {
    let vfs = SimVfs::new(23);
    let rows: Vec<Vec<u8>> = (0..16).map(|i| format!("row{i:02}").into_bytes()).collect();
    let rows: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"row00", &rows, b"kept")
            .prepare(b"c1", Durability::GroupSync)
            .unwrap();
        // Reading a row c1 writes is refused at once while c1 is in doubt (D329).
        assert!(matches!(
            txn(&db, &t, b"row00", &[b"other"], b"no").prepare(b"r1", Durability::GroupSync),
            Err(Error::Conflict)
        ));
        txn(&db, &t, b"other", &[b"other"], b"dropped")
            .prepare(b"a1", Durability::GroupSync)
            .unwrap();
        // The close doesn't wait for the decisions: they're the caller's.
        db.close().unwrap();
    }
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    let mut doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
    doubt.sort();
    assert_eq!(doubt, [b"a1".to_vec(), b"c1".to_vec()]);
    assert!(rows.iter().all(|r| get_bytes(&db, &t, r).is_none()));
    // The holds are back: a PREPARE over the same rows is refused while they're in doubt.
    assert!(
        txn(&db, &t, b"row01", &rows[..1], b"overlap")
            .prepare(b"o1", Durability::GroupSync)
            .is_err()
    );
    db.decide(b"c1", true).unwrap().expect("in doubt");
    db.decide(b"a1", false).unwrap().expect("in doubt");
    assert!(
        rows.iter()
            .all(|r| get_bytes(&db, &t, r).as_deref() == Some(&b"kept"[..]))
    );
    assert_eq!(get_bytes(&db, &t, b"other"), None);
    assert!(db.in_doubt().is_empty());
    db.close().unwrap();
    // Decided before the close: a reopen finds nothing in doubt, and the commit stands.
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    assert!(db.in_doubt().is_empty());
    assert_eq!(get_bytes(&db, &t, b"row05").as_deref(), Some(&b"kept"[..]));
    db.close().unwrap();
}

#[test]
fn a_reopen_with_another_shard_count_is_refused_while_shares_are_in_doubt() {
    let vfs = SimVfs::new(24);
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"a", &[b"a", b"z"], b"v")
            .prepare(b"x", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
    }
    assert!(matches!(
        Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 3)),
        Err(Error::Unsupported(_))
    ));
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    db.decide(b"x", false).unwrap().expect("in doubt");
    db.close().unwrap();
    // Decided: any shard count opens.
    Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 3))
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn past_the_in_doubt_limits_a_prepare_is_busy() {
    let vfs = SimVfs::new(25);
    let mut o = owned(Arc::clone(&vfs), 2);
    o.in_doubt_limits = (2, 0);
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for (i, xid) in [&b"l1"[..], b"l2"].into_iter().enumerate() {
        let row = format!("r{i}");
        txn(&db, &t, row.as_bytes(), &[row.as_bytes()], b"v")
            .prepare(xid, Durability::GroupSync)
            .unwrap();
    }
    assert!(matches!(
        txn(&db, &t, b"r9", &[b"r9"], b"v").prepare(b"l3", Durability::GroupSync),
        Err(Error::Busy)
    ));
    // One decided frees its place.
    db.decide(b"l1", false).unwrap();
    txn(&db, &t, b"r9", &[b"r9"], b"v")
        .prepare(b"l3", Durability::GroupSync)
        .unwrap();
    db.decide(b"l2", true).unwrap();
    db.decide(b"l3", true).unwrap();
    db.close().unwrap();
}

/// perf2's E2 on ICR 0140: recovery counts a type-6 abort's own seqno (and a type-5 id), so
/// a reopen after an abort that was the last record hands out seqnos above it.
#[test]
fn a_reopen_after_an_abort_hands_out_seqnos_above_it() {
    let vfs = SimVfs::new(26);
    let abort = {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"a", &[b"a", b"z"], b"v")
            .prepare(b"e2", Durability::GroupSync)
            .unwrap();
        let abort = db.decide(b"e2", false).unwrap().expect("in doubt").seqno;
        // Dropped without a clean close: the reopen replays the WAL.
        drop(db);
        abort
    };
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    assert!(db.in_doubt().is_empty());
    assert_eq!(get_bytes(&db, &t, b"a"), None, "the abort applied nothing");
    let mut wb = WriteBatch::new();
    let f = t.family("f").unwrap().id;
    wb.put(t.id, f, b"a", b"q", None, ValueRef::Bytes(b"after"))
        .unwrap();
    let next = db.commit(wb, Some(Durability::GroupSync)).unwrap().seqno;
    assert!(next > abort, "{next} after the abort's {abort}");
    db.close().unwrap();
}

/// A read-only open of a database with a share in doubt: the share stays invisible, it is
/// listed, and deciding it is refused (`ReadOnly`); the writer's next open decides it.
#[test]
fn a_read_only_open_lists_a_share_in_doubt_and_cannot_decide_it() {
    let vfs = SimVfs::new(27);
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"r", &[b"r"], b"v")
            .prepare(b"ro", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
    }
    {
        let mut o = owned(Arc::clone(&vfs), 2);
        o.read_only = true;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let t = db.table("t").expect("table t");
        assert_eq!(get_bytes(&db, &t, b"r"), None);
        let doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
        assert_eq!(doubt, [b"ro".to_vec()]);
        assert!(matches!(db.decide(b"ro", true), Err(Error::ReadOnly)));
        db.close().unwrap();
    }
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    db.decide(b"ro", true).unwrap().expect("in doubt");
    assert_eq!(get_bytes(&db, &t, b"r").as_deref(), Some(&b"v"[..]));
    db.close().unwrap();
}

/// perf2's M2 on #2000: a commit right behind an external abort becomes visible on an
/// otherwise idle database. The abort's seqno is held until its type 6 is written; its
/// release must wake the commit's visibility wait, or nothing later would.
/// `Fault::HoldDecisionRecords` keeps the type 6 unwritten while the commit, reserved after
/// the abort's seqno, applies and waits.
#[test]
fn a_commit_right_behind_an_abort_becomes_visible_on_an_idle_database() {
    let vfs = SimVfs::new(28);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    txn(&db, &t, b"held", &[b"held"], b"v")
        .prepare(b"m2", Durability::GroupSync)
        .unwrap();
    db.inject(Fault::HoldDecisionRecords, true);
    let decider = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.decide(b"m2", false))
    };
    // The coordinator took the decision (and reserved the abort's seqno) once the xid has
    // left the registry; its type 6 is held.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !db.in_doubt().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the decide never reached its coordinator"
        );
        std::thread::yield_now();
    }
    let (done, rx) = std::sync::mpsc::channel();
    let committer = {
        let db = Arc::clone(&db);
        let table = t.id;
        std::thread::spawn(move || {
            let mut wb = WriteBatch::new();
            wb.put(table, f, b"free", b"q", None, ValueRef::Bytes(b"v"))
                .unwrap();
            let c = db.commit(wb, Some(Durability::GroupSync)).unwrap();
            done.send(c.seqno).unwrap();
        })
    };
    // Behind the held abort, the commit can't become visible.
    assert!(
        rx.recv_timeout(std::time::Duration::from_millis(200))
            .is_err()
    );
    db.release_held_decisions();
    let seqno = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the commit behind the abort never became visible");
    let abort = decider.join().unwrap().unwrap().expect("in doubt");
    assert!(
        seqno > abort.seqno,
        "the commit {seqno} is behind the abort {}",
        abort.seqno
    );
    committer.join().unwrap();
    db.close().unwrap();
}

/// perf2's S2 on #2000: a decide once the database is closing is refused (`Closed`), and the
/// share stays in doubt for the next open, its PREPAREs kept.
#[test]
fn a_decide_after_the_close_is_refused_and_the_share_stays_in_doubt() {
    let vfs = SimVfs::new(29);
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"s", &[b"s"], b"v")
            .prepare(b"s2", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
        assert!(db.decide(b"s2", true).is_err(), "a decide after the close");
    }
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let doubt: Vec<Vec<u8>> = db.in_doubt().into_iter().map(|(x, _)| x).collect();
    assert_eq!(doubt, [b"s2".to_vec()]);
    db.decide(b"s2", false).unwrap().expect("in doubt");
    db.close().unwrap();
}

/// Polls a maintenance future to its end; the engine's own threads do the work.
fn finish(mut m: pigeonhole_engine::PendingMaintenance) -> pigeonhole_engine::Result<()> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Poll::Ready(r) = std::pin::Pin::new(&mut m).poll(&mut cx) {
            return r;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the maintenance never finished"
        );
        std::thread::yield_now();
    }
}

/// perf2's M1 on #2000: tablet owners aren't persisted, so a tablet moved before a prepare
/// would be placed elsewhere at the reopen than the share's holds. The open pins it to the
/// shard that logged the share: a write to its row is still refused while the share is in
/// doubt, and the decide applies there.
#[test]
fn a_reopen_keeps_an_in_doubt_shares_moved_tablet_on_its_shard() {
    let vfs = SimVfs::new(30);
    let moved_to = {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let from = db.shard_of(t.id, b"r").expect("routed");
        let to = 1 - from;
        finish(db.move_tablet_pending(t.id, b"r", to as u16).unwrap()).unwrap();
        assert_eq!(db.shard_of(t.id, b"r"), Some(to));
        txn(&db, &t, b"r", &[b"r"], b"held")
            .prepare(b"m1", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
        to
    };
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    assert_eq!(
        db.shard_of(t.id, b"r"),
        Some(moved_to),
        "the tablet is placed on the shard that logged the share"
    );
    // The holds are on the tablet's owner: a write to the row is refused while in doubt.
    let f = t.family("f").unwrap().id;
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"other"))
        .unwrap();
    let w = db.commit(wb, Some(Durability::GroupSync));
    assert!(matches!(w, Err(Error::Busy)), "{w:?}");
    db.decide(b"m1", true).unwrap().expect("in doubt");
    assert_eq!(get_bytes(&db, &t, b"r").as_deref(), Some(&b"held"[..]));
    db.close().unwrap();
}

/// The read holds survive a reopen: a share logs the reads its shard validates, routed by
/// the tablet map its group validated with. Before #2000's fix the share was encoded on the
/// message's arrival, whose map hadn't the just-created table yet, so it logged no reads
/// and, after the reopen, a write to a row it read was admitted.
#[test]
fn a_shares_read_holds_survive_a_reopen() {
    let vfs = SimVfs::new(32);
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        txn(&db, &t, b"read", &[b"written"], b"v")
            .prepare(b"rh", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
    }
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    let f = t.family("f").unwrap().id;
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"read", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    let w = db.commit(wb, Some(Durability::GroupSync));
    assert!(
        matches!(w, Err(Error::Busy)),
        "a write to a row it read: {w:?}"
    );
    db.decide(b"rh", false).unwrap().expect("in doubt");
    db.close().unwrap();
}

/// The non-blocking forms (for the model-check harness and async callers): `submit_prepare`
/// and `submit_decide` resolve as the blocking calls return, and a refused prepare whose
/// `PendingCommit` is dropped unread still releases its reservation when its coordination's
/// last handle goes (`ExternalReq`'s drop), so the xid prepares again.
#[test]
fn the_non_blocking_forms_resolve_and_a_dropped_refusal_releases_its_xid() {
    let vfs = SimVfs::new(33);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let p = txn(&db, &t, b"a", &[b"a", b"z"], b"v")
        .submit_prepare(b"nb", Durability::GroupSync)
        .unwrap()
        .wait()
        .unwrap();
    assert!(p.seqno > 0);
    let d = db
        .submit_decide(b"nb", true)
        .unwrap()
        .expect("in doubt")
        .wait()
        .unwrap();
    assert!(d.seqno > p.seqno);
    assert!(db.submit_decide(b"nb", true).unwrap().is_none(), "decided");

    // A prepare refused (`Conflict`: a row it read changed), its answer never read.
    let stale = txn(&db, &t, b"r", &[b"other"], b"v");
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, b"r", b"q", None, ValueRef::Bytes(b"changed"))
        .unwrap();
    db.commit(wb, None).unwrap();
    drop(
        stale
            .submit_prepare(b"again", Durability::GroupSync)
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match txn(&db, &t, b"q", &[b"q"], b"v").prepare(b"again", Durability::GroupSync) {
            Ok(_) => break,
            Err(Error::InvalidArgument(_)) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the reservation was never released"
                );
                std::thread::yield_now();
            }
            Err(e) => panic!("{e:?}"),
        }
    }
    db.decide(b"again", false).unwrap().expect("in doubt");
    db.close().unwrap();
}

/// perf2's M1 re-review on #2000: tablet changes are each open's choice. A tablet moved by
/// an earlier open and held by an in-doubt share can't go back to `shard_for`'s shard, so
/// an open with tablet changes off is refused; one with them on places it on its share's
/// shard, and decides it.
#[test]
fn an_open_without_tablet_changes_is_refused_while_a_moved_tablet_is_held() {
    let vfs = SimVfs::new(34);
    {
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let to = 1 - db.shard_of(t.id, b"r").expect("routed");
        finish(db.move_tablet_pending(t.id, b"r", to as u16).unwrap()).unwrap();
        txn(&db, &t, b"r", &[b"r"], b"held")
            .prepare(b"off", Durability::GroupSync)
            .unwrap();
        db.close().unwrap();
    }
    let mut off = owned(Arc::clone(&vfs), 2);
    off.tablet_changes = false;
    assert!(matches!(
        Engine::open(Path::new(DB), off),
        Err(Error::Unsupported(_))
    ));
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").expect("table t");
    db.decide(b"off", true).unwrap().expect("in doubt");
    assert_eq!(get_bytes(&db, &t, b"r").as_deref(), Some(&b"held"[..]));
    db.close().unwrap();
}

/// A share in doubt is logged, but a `None` decision writes no record, so a crash could
/// bring a decided share back in doubt: an external prepare at `None` is refused (found by
/// the model-check harness's external ops).
#[test]
fn an_external_prepare_at_none_is_refused() {
    let vfs = SimVfs::new(35);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    assert!(matches!(
        txn(&db, &t, b"n", &[b"n"], b"v").prepare(b"none", Durability::None),
        Err(Error::InvalidArgument(_))
    ));
    assert!(db.in_doubt().is_empty());
    // Refused before reserving: the xid prepares at a logged level.
    txn(&db, &t, b"n", &[b"n"], b"v")
        .prepare(b"none", Durability::Buffered)
        .unwrap();
    db.decide(b"none", false).unwrap().expect("in doubt");
    db.close().unwrap();
}

/// A plain commit across shards over a row an in-doubt share holds is refused `Busy`, as a
/// single is: it read nothing, so `Conflict` would be wrong (found by the model-check
/// harness, where a plain commit's `Conflict` is a protocol failure).
#[test]
fn a_plain_cross_shard_commit_over_a_held_row_is_busy_not_conflict() {
    let vfs = SimVfs::new(36);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    // A row of another table on the other shard, so the plain commit is cross-shard.
    let u = db
        .create_table("u", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let uf = u.family("f").unwrap().id;
    let (held, other) = (b"held".to_vec(), b"other".to_vec());
    let here = db.shard_of(t.id, &held).expect("routed");
    if db.shard_of(u.id, &other) == Some(here) {
        finish(
            db.move_tablet_pending(u.id, &other, (1 - here) as u16)
                .unwrap(),
        )
        .unwrap();
    }
    txn(&db, &t, &held, &[held.as_slice()], b"v")
        .prepare(b"pl", Durability::GroupSync)
        .unwrap();
    let mut wb = WriteBatch::new();
    wb.put(t.id, f, &held, b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    wb.put(u.id, uf, &other, b"q", None, ValueRef::Bytes(b"y"))
        .unwrap();
    let w = db.commit(wb, Some(Durability::GroupSync));
    assert!(matches!(w, Err(Error::Busy)), "{w:?}");
    db.decide(b"pl", false).unwrap().expect("in doubt");
    db.close().unwrap();
}
