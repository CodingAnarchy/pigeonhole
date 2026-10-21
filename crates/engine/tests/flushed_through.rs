//! #724 area 4: each shard publishes a flushed-through seqno (`Shared::flushed_through`), the
//! largest seqno at or below which every write it will hold is in a durable SST. It is built
//! from the unflushed memtables only, capped by the visible watermark, published after a
//! flush's root commit, at `Start`, and when an unpin finds nothing to flush (perf287's
//! review), and never decreases. The model checker asserts the bound at every crash
//! (`common::World::check_flushed_through`).

mod common;

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::{Durability, Seqno};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/f.phdb";

fn open() -> Arc<Engine> {
    let vfs = SimVfs::new(724);
    let mut o = common::options(Arc::clone(&vfs), 2, 64 << 20);
    o.tablet_changes = false;
    Engine::open(Path::new(DB), o).unwrap()
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(b"v"),
    )
    .unwrap();
    wb
}

/// Commits `row` to `t` and returns its seqno and the shard whose stream logged it.
fn commit(db: &Engine, t: &TableInfo, row: &str) -> (Seqno, usize) {
    db.record_history(true);
    db.take_appended();
    let info = db.commit(put(t, row), Some(Durability::Sync)).unwrap();
    let shard = db.take_appended().last().expect("one record").stream;
    (info.seqno, usize::from(shard))
}

/// Two tables, one per shard (placement is round-robin).
fn tables(db: &Engine) -> (Arc<TableInfo>, Arc<TableInfo>) {
    let a = db
        .create_table("a", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let b = db
        .create_table("b", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    (a, b)
}

/// Waits (real shard threads) until `ok`, at most 10 s.
fn wait_until(what: &str, ok: impl Fn() -> bool) {
    let start = Instant::now();
    while !ok() {
        assert!(start.elapsed() < Duration::from_secs(10), "{what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn a_flush_raises_the_flushed_through_seqno_past_its_writes_but_not_past_an_unflushed_one() {
    // The bound passes a write once a flush puts it in an SST, stays at or below the visible
    // watermark, never goes back, and stays below a later unflushed write. (Building it from
    // `MemSlot::unflushed` rather than every memtable, perf287's first change, matters for a
    // flushed memtable still listed: a slot sealed `Flushed` with older frozen memtables left,
    // or a frozen one already in `flushed_roots` before `on_flushed` drops it. `Engine::flush`
    // reaches neither, so this test does not tell the two apart; the model checker's crash
    // assertion holds for both.)
    let db = open();
    let (a, _) = tables(&db);
    let (first, s) = commit(&db, &a, "r1");
    assert!(db.flushed_through(s) < first);
    db.flush().unwrap();
    wait_until("the flush never published past the first write", || {
        db.flushed_through(s) >= first
    });
    let f = db.flushed_through(s);
    assert!(
        f <= db.snapshot().unwrap().seqno(),
        "above the visible watermark: {f}"
    );
    let (second, s2) = commit(&db, &a, "r2");
    assert_eq!(s2, s);
    // Unflushed: the bound stays below it (and never went back).
    std::thread::sleep(Duration::from_millis(20));
    let g = db.flushed_through(s);
    assert!(g >= f && g < second, "{f} -> {g}, second write at {second}");
    db.flush().unwrap();
    wait_until(
        "the second flush never published past the second write",
        || db.flushed_through(s) >= second,
    );
    db.close().unwrap();
}

#[test]
fn an_unpin_with_nothing_to_flush_publishes_the_flushed_through_seqno() {
    // perf287's second change: a shard asked to unpin through S that holds nothing
    // unflushed at or below S starts no flush, so it publishes the bound itself; otherwise
    // an idle participant (a validation-only share, say) would never raise it.
    let db = open();
    let (a, b) = tables(&db);
    let (_, sa) = commit(&db, &a, "probe");
    let (_, sb) = commit(&db, &b, "probe");
    assert_ne!(sa, sb);
    // Flush b's shard, then write only to a: b's shard is idle, its bound stuck at its last
    // publish while the visible watermark moves on.
    db.flush().unwrap();
    let mut last = 0;
    for i in 0..20 {
        last = commit(&db, &a, &format!("r{i}")).0;
    }
    let before = db.flushed_through(sb);
    assert!(
        before < last,
        "b's shard already published {before} >= {last}"
    );
    db.request_unpin(sb, last).unwrap();
    wait_until("the unpin never published b's bound", || {
        db.flushed_through(sb) >= last
    });
    db.close().unwrap();
}

#[test]
fn an_unpin_between_a_flushs_root_commit_and_its_flushed_turn_counts_the_memtable_flushed() {
    // perf287's test for `MemSlot::unflushed`: the flushed-through seqno counts a frozen
    // memtable as flushed once the view stores its root in `flushed_roots`, before the shard
    // handles `Flushed` and drops it. One application-owned shard: its flush writes the SST
    // and queues its manifest request behind the exclusion this thread holds; an unpin
    // through the writes is queued; the test thread commits the manifest (the root lands in
    // `flushed_roots`, `Flushed` queues behind the `Unpin`). The unpin, run first, finds
    // nothing unflushed at or below its seqno and publishes past the writes. Counting every
    // memtable instead, it would find the frozen one and publish nothing.
    let vfs = SimVfs::new(7241);
    let mut o = pigeonhole_engine::EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.reader_slots = 4;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/u.phdb"), o).expect("open");
    let mut shard = shards.remove(0);
    let step = |shard: &mut pigeonhole_engine::EngineShard| {
        for _ in 0..64 {
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            if !shard.run_once(now + 1_000_000) {
                break;
            }
        }
    };
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut last = 0;
    for i in 0..8 {
        let mut pc = engine
            .submit(put(&t, &format!("r{i}")), Some(Durability::Buffered))
            .unwrap();
        step(&mut shard);
        let std::task::Poll::Ready(r) = std::pin::Pin::new(&mut pc)
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        else {
            panic!("r{i} did not commit");
        };
        last = r.unwrap().seqno;
    }
    assert!(engine.hold_manifest());
    let _flush = engine.flush_pending().unwrap();
    step(&mut shard);
    engine.request_unpin(0, last).unwrap();
    engine.drain_manifest();
    assert_eq!(engine.unpin_published(0), 0);
    step(&mut shard);
    assert!(
        engine.unpin_published(0) >= last,
        "the unpin published {} (writes through {last}): it counted the flushed memtable",
        engine.unpin_published(0)
    );
    engine.release_manifest();
    step(&mut shard);
    assert!(engine.flushed_through(0) >= last);
    engine.close().unwrap();
    step(&mut shard);
}
