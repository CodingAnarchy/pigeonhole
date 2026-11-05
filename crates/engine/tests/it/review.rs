//! Reproducers from the Milestone A review: aborted cross-shard commits must resolve (and
//! never pin the watermark), optimistic transactions must not allow write skew across
//! shards, conditional writes see the applied state behind same-group writes and pending
//! shares, and a participant that fails to apply never lets the coordinator ack `Ok`.

use crate::common;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, Predicate, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{FaultPlan, SimVfs};

const DB: &str = "/db/data.phdb";

fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 4 << 20);
    o.pin_threads = false;
    o
}

/// Two tables on two shards (tablet 1 → shard 1, tablet 2 → shard 0).
fn setup(seed: u64) -> (Arc<SimVfs>, Arc<Engine>, Vec<Arc<TableInfo>>) {
    let vfs = SimVfs::new(seed);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let a = db
        .create_table("a", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let b = db
        .create_table("b", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    (vfs, db, vec![a, b])
}

/// Runs `f` on a thread and panics if it has not finished within five seconds.
fn within<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    within_for(what, Duration::from_secs(5), f)
}

/// [`within`] with its own hang limit.
fn within_for<T: Send + 'static>(
    what: &str,
    limit: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(limit) {
        Ok(r) => r,
        Err(_) => panic!("{what} never resolved (a hang)"),
    }
}

fn put(t: &TableInfo, row: &[u8], v: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(t.id, t.families[0].id, row, b"q", None, ValueRef::Bytes(v))
        .unwrap();
    wb
}

fn get(db: &Engine, t: &TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    db.get(&snap, t.id, t.families[0].id, row, b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

#[test]
fn cross_shard_txn_conflict_resolves() {
    let (_vfs, db, t) = setup(10);
    let (a, b) = (&t[0], &t[1]);
    let (fa, fb) = (a.families[0].id, b.families[0].id);
    let mut txn = db.begin().unwrap();
    let _ = txn.get(a.id, fa, b"x", b"q").unwrap();
    db.commit(put(a, b"x", b"1"), None).unwrap();
    txn.batch()
        .put(a.id, fa, b"x", b"q", None, ValueRef::Bytes(b"2"))
        .unwrap();
    txn.batch()
        .put(b.id, fb, b"y", b"q", None, ValueRef::Bytes(b"2"))
        .unwrap();
    let r = within("cross-shard txn with a conflict", move || txn.commit(None));
    assert!(matches!(r, Err(Error::Conflict)), "{r:?}");
    assert_eq!(get(&db, a, b"x").as_deref(), Some(&b"1"[..]));
    assert_eq!(
        get(&db, b, b"y"),
        None,
        "aborted: nothing applied on either shard"
    );
    // A later commit becomes visible (no held seqno pins the watermark).
    let db2 = Arc::clone(&db);
    let a2 = Arc::clone(a);
    let r = within("a later commit", move || {
        db2.commit(put(&a2, b"z", b"1"), None)
    });
    r.unwrap();
    assert_eq!(get(&db, a, b"z").as_deref(), Some(&b"1"[..]));
    within("close after an abort", move || db.close()).unwrap();
}

#[test]
fn cross_shard_write_skew_is_prevented() {
    let (_vfs, db, t) = setup(11);
    let (a, b) = (&t[0], &t[1]);
    let (fa, fb) = (a.families[0].id, b.families[0].id);
    let mut t1 = db.begin().unwrap();
    let mut t2 = db.begin().unwrap();
    let _ = t1.get(a.id, fa, b"x", b"q").unwrap(); // t1 reads a/x
    let _ = t2.get(b.id, fb, b"y", b"q").unwrap(); // t2 reads b/y
    t1.batch()
        .put(b.id, fb, b"y", b"q", None, ValueRef::Bytes(b"1"))
        .unwrap(); // writes b/y
    t2.batch()
        .put(a.id, fa, b"x", b"q", None, ValueRef::Bytes(b"1"))
        .unwrap(); // writes a/x
    let r1 = within("t1", move || t1.commit(None));
    let r2 = within("t2", move || t2.commit(None));
    assert!(
        r1.is_err() || r2.is_err(),
        "write skew: both committed ({r1:?}, {r2:?})"
    );
    assert!(
        r1.is_ok() || r2.is_ok(),
        "one of them should commit ({r1:?}, {r2:?})"
    );
    db.close().unwrap();
}

#[test]
fn aborts_from_busy_poison_and_close_all_resolve() {
    // BatchTooLarge: a participant whose share can never fit its arena (even empty) refuses
    // its PREPARE; the whole commit aborts with that non-retryable error (issue #141). A
    // merely full arena waits for a flush instead.
    {
        let vfs = SimVfs::new(12);
        let mut o = owned(Arc::clone(&vfs), 2);
        o.memtable_budget = 512 << 10;
        o.memtable_freeze_bytes = 64 << 10;
        // Segments large enough that the share below is refused for its arena, not its size.
        o.wal.segment_size = 4 << 20;
        let db = Engine::open(Path::new(DB), o).unwrap();
        let a = db
            .create_table("a", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let b = db
            .create_table("b", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        // Table b's shard keeps absorbing writes: full arenas flush.
        for i in 0..60u32 {
            db.commit(
                put(&b, &i.to_be_bytes(), &vec![1u8; 16 << 10]),
                Some(Durability::None),
            )
            .unwrap();
        }
        assert!(
            db.metrics().flushes > 0,
            "the arena was flushed, not refused"
        );
        // Larger than the whole arena (the region rounds the budget up to a few MiB).
        let mut wb = put(&a, b"x", b"small");
        for i in 0..200u32 {
            wb.put(
                b.id,
                b.families[0].id,
                &i.to_be_bytes(),
                b"big",
                None,
                ValueRef::Bytes(&vec![2u8; 16 << 10]),
            )
            .unwrap();
        }
        let db2 = Arc::clone(&db);
        let r = within("cross-shard commit with a busy participant", move || {
            db2.commit(wb, None)
        });
        assert!(matches!(r, Err(Error::BatchTooLarge)), "{r:?}");
        assert_eq!(
            get(&db, &a, b"x"),
            None,
            "nothing applied on the other shard"
        );
        let db2 = Arc::clone(&db);
        let a2 = Arc::clone(&a);
        within("a later commit on the free shard", move || {
            db2.commit(put(&a2, b"y", b"1"), None)
        })
        .unwrap();
        within("close", move || db.close()).unwrap();
    }
    // Poisoned: an I/O error on one participant's stream aborts the commit, and every
    // later commit on that shard fails until reopen, while the other shard keeps going.
    {
        let (vfs, db, t) = setup(13);
        let (a, b) = (&t[0], &t[1]);
        db.commit(put(a, b"x", b"0"), None).unwrap();
        db.commit(put(b, b"y", b"0"), None).unwrap();
        let mut plan = FaultPlan::none();
        plan.enospc_after_bytes = Some(0);
        vfs.set_faults(plan);
        let mut wb = put(a, b"x", b"1");
        wb.put(
            b.id,
            b.families[0].id,
            b"y",
            b"q",
            None,
            ValueRef::Bytes(b"1"),
        )
        .unwrap();
        let db2 = Arc::clone(&db);
        let r = within("cross-shard commit with a failing stream", move || {
            db2.commit(wb, None)
        });
        assert!(r.is_err(), "{r:?}");
        vfs.set_faults(FaultPlan::none());
        assert_eq!(get(&db, a, b"x").as_deref(), Some(&b"0"[..]));
        assert_eq!(
            get(&db, b, b"y").as_deref(),
            Some(&b"0"[..]),
            "atomic: neither half applied"
        );
        let db2 = Arc::clone(&db);
        let a2 = Arc::clone(a);
        let r = within("a later commit on the poisoned shard", move || {
            db2.commit(put(&a2, b"x", b"2"), None)
        });
        // Refused before logging anything (#921): `ShardFailed`, shown as an I/O error.
        assert!(matches!(r, Err(Error::ShardFailed)), "{r:?}");
        within("close with a poisoned shard", move || db.close()).ok();
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
        let a = db.table("a").unwrap();
        assert_eq!(get(&db, &a, b"x").as_deref(), Some(&b"0"[..]));
        db.close().unwrap();
    }
    // Close racing a cross-shard commit: whichever wins, both resolve.
    {
        let (_vfs, db, t) = setup(14);
        let (a, b) = (&t[0], &t[1]);
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let db = Arc::clone(&db);
            let (a, b) = (Arc::clone(a), Arc::clone(b));
            handles.push(std::thread::spawn(move || {
                let mut wb = put(&a, &[i], b"1");
                wb.put(
                    b.id,
                    b.families[0].id,
                    &[i],
                    b"q",
                    None,
                    ValueRef::Bytes(b"1"),
                )
                .unwrap();
                db.commit(wb, Some(Durability::Buffered))
            }));
        }
        let db2 = Arc::clone(&db);
        let close = std::thread::spawn(move || db2.close());
        let results: Vec<_> = handles
            .into_iter()
            .map(|h| within("commit vs close", move || h.join().unwrap()))
            .collect();
        within("close vs commits", move || close.join().unwrap()).unwrap();
        for r in results {
            assert!(matches!(r, Ok(_) | Err(Error::Closed)), "{r:?}");
        }
    }
}

/// [`conditional_writes_see_same_group_writes_and_pending_shares`]'s liveness floor: at least
/// this many CAS calls (and, after them, transactions) apply in all.
const PROGRESS: u64 = 8;

/// How long its threads keep trying past their 25 attempts each to reach [`PROGRESS`].
const PROGRESS_FOR: Duration = Duration::from_secs(10);

#[test]
fn conditional_writes_see_same_group_writes_and_pending_shares() {
    let (_vfs, db, t) = setup(15);
    let (a, b) = (&t[0], &t[1]);
    let f = a.families[0].id;
    // A counter incremented only through compare-and-set (read the latest value, claim
    // "value == what I read" → write value + 1), by eight threads at once, while another
    // thread keeps writing a different column of the same row (same-group deferrals) and
    // cross-shard commits touch the row as prepared shares. Every applied CAS saw the
    // applied state, so the final value equals the number of applied CAS calls.
    db.commit(put(a, b"ctr", &0u64.to_le_bytes()), None)
        .unwrap();
    let db2 = Arc::clone(&db);
    let (a2, b2) = (Arc::clone(a), Arc::clone(b));
    // Each thread makes its 25 attempts, then keeps trying while fewer than `PROGRESS` have
    // applied in all, for at most `PROGRESS_FOR`: a failed CAS read a value the predicate's
    // applied-state read had already moved past (a stale visible read, while a cross-shard
    // round holds visibility back), and on a starved runner those windows can cover most of
    // a thread's attempts (applied 4 of 200 once on CI, against 25-73 measured).
    let deadline = Instant::now() + PROGRESS_FOR;
    let total = Arc::new(AtomicU64::new(0));
    let total2 = Arc::clone(&total);
    let applied = within_for("races", PROGRESS_FOR + Duration::from_secs(5), move || {
        std::thread::scope(|s| {
            let mut hs = Vec::new();
            for _ in 0..8 {
                let db = Arc::clone(&db2);
                let a = Arc::clone(&a2);
                let total = Arc::clone(&total2);
                hs.push(s.spawn(move || {
                    let mut applied = 0u64;
                    let mut attempts = 0u32;
                    while attempts < 25
                        || (total.load(Ordering::Relaxed) < PROGRESS && Instant::now() < deadline)
                    {
                        attempts += 1;
                        let v = db.get_latest(a.id, f, b"ctr", b"q").unwrap().unwrap();
                        let n = u64::from_le_bytes(v.stored()[1..].try_into().unwrap());
                        let pred = Predicate::Value {
                            family: f,
                            qualifier: b"q".to_vec(),
                            predicate: pigeonhole_engine::ValuePredicate::Equals(
                                n.to_le_bytes().to_vec(),
                            ),
                        };
                        let batch = put(&a, b"ctr", &(n + 1).to_le_bytes());
                        if db
                            .check_and_mutate(
                                a.id,
                                b"ctr",
                                &pred,
                                batch,
                                Some(Durability::Buffered),
                            )
                            .unwrap()
                            .0
                        {
                            applied += 1;
                            total.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    applied
                }));
            }
            let db = Arc::clone(&db2);
            let (a, b) = (Arc::clone(&a2), Arc::clone(&b2));
            let noise = s.spawn(move || {
                for i in 0..50u32 {
                    let mut wb = WriteBatch::new();
                    wb.put(
                        a.id,
                        f,
                        b"ctr",
                        b"other",
                        None,
                        ValueRef::Bytes(&i.to_le_bytes()),
                    )
                    .unwrap();
                    wb.put(
                        b.id,
                        b.families[0].id,
                        b"x",
                        b"q",
                        None,
                        ValueRef::Bytes(b"cross"),
                    )
                    .unwrap();
                    db.commit(wb, Some(Durability::Buffered)).unwrap();
                }
            });
            let total: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
            noise.join().unwrap();
            total
        })
    });
    let v = db.get_latest(a.id, f, b"ctr", b"q").unwrap().unwrap();
    let n = u64::from_le_bytes(v.stored()[1..].try_into().unwrap());
    assert_eq!(n, applied, "lost or phantom increments");
    assert!(applied >= PROGRESS, "{applied}");
    // The same invariant through optimistic transactions (read, then write n + 1).
    let db2 = Arc::clone(&db);
    let a2 = Arc::clone(a);
    // As above: 25 attempts each, then on while fewer than `PROGRESS` committed in all.
    let deadline = Instant::now() + PROGRESS_FOR;
    let total = Arc::new(AtomicU64::new(0));
    let total2 = Arc::clone(&total);
    let committed = within_for(
        "txn races",
        PROGRESS_FOR + Duration::from_secs(5),
        move || {
            std::thread::scope(|s| {
                let mut hs = Vec::new();
                for _ in 0..8 {
                    let db = Arc::clone(&db2);
                    let a = Arc::clone(&a2);
                    let total = Arc::clone(&total2);
                    hs.push(s.spawn(move || {
                        let mut ok = 0u64;
                        let mut attempts = 0u32;
                        while attempts < 25
                            || (total.load(Ordering::Relaxed) < PROGRESS
                                && Instant::now() < deadline)
                        {
                            attempts += 1;
                            let mut txn = db.begin().unwrap();
                            let v = txn.get(a.id, f, b"ctr", b"q").unwrap().unwrap();
                            let n = u64::from_le_bytes(v.stored()[1..].try_into().unwrap());
                            txn.batch()
                                .put(
                                    a.id,
                                    f,
                                    b"ctr",
                                    b"q",
                                    None,
                                    ValueRef::Bytes(&(n + 1).to_le_bytes()),
                                )
                                .unwrap();
                            match txn.commit(Some(Durability::Buffered)) {
                                Ok(_) => {
                                    ok += 1;
                                    total.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(Error::Conflict) => {}
                                Err(e) => panic!("{e}"),
                            }
                        }
                        ok
                    }));
                }
                hs.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
            })
        },
    );
    let v = db.get_latest(a.id, f, b"ctr", b"q").unwrap().unwrap();
    let n = u64::from_le_bytes(v.stored()[1..].try_into().unwrap());
    assert_eq!(n, applied + committed, "lost update through transactions");
    assert!(committed >= PROGRESS, "{committed}");
    // Transactions whose reads overlap a same-group write must abort or see it.
    let mut txn = db.begin().unwrap();
    let before = txn
        .get(a.id, f, b"r", b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()));
    db.commit(put(a, b"r", b"final"), Some(Durability::Buffered))
        .unwrap();
    txn.batch()
        .put(a.id, f, b"r", b"q", None, ValueRef::Bytes(b"txn"))
        .unwrap();
    let r = within("txn behind a write", move || txn.commit(None));
    assert!(
        matches!(r, Err(Error::Conflict)),
        "read {before:?}, got {r:?}"
    );
    assert_eq!(get(&db, a, b"r").as_deref(), Some(&b"final"[..]));
    db.close().unwrap();
}
