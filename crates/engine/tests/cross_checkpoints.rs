//! D266: a cross-shard commit's PREPARE and COMMIT records leave their logs once every
//! shard holding its writes has published a flushed-through seqno at or above it, with no
//! report per share (D254's `SharesFlushed`/`CommitsSettled`, which took a 16-shard flush
//! install about 6 ms on the shard thread, #777). A checkpoint stopped at a cross record
//! asks each peer behind it once (`AwaitFlushed`) and is answered once (`FlushedThrough`):
//! messages per blocked peer, not per share. The checkpoints still pass every record, and a
//! power loss after them keeps every write.
//!
//! The second test runs without tablet changes, so no later manifest commit makes a shard
//! look at its checkpoint again: every record must pass on the flush's own messages.
//!
//! Application-owned with every shard driven by the test, so each run is deterministic.

mod common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, FamilyId};
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";
/// Large enough that nothing flushes until the test asks.
const BUDGET: u64 = 64 << 20;

fn options(vfs: &Arc<SimVfs>, tablet_changes: bool) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 3, BUDGET);
    o.tablet_changes = tablet_changes;
    o
}

/// Runs every shard until `pending` resolves.
fn drive<F: Future + Unpin>(shards: &mut [EngineShard], mut pending: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..1_000_000 {
        for s in shards.iter_mut() {
            s.run_once(u64::MAX);
        }
        if let Poll::Ready(r) = Pin::new(&mut pending).poll(&mut cx) {
            return r;
        }
    }
    panic!("never resolved");
}

/// Runs every shard until all of them go idle: a pass in which no shard has work left or did
/// any. `run_once` says whether work remains after its turn, not whether it did any: the
/// last shard of a pass can message an earlier one and drain its own kick in the same turn.
/// So a pass is quiet only if no shard's `EngineShard::work_count` moved either: then it
/// handled nothing, sent nothing, and left nothing queued.
fn settle(shards: &mut [EngineShard]) {
    for _ in 0..100_000 {
        let work: u64 = shards.iter().map(EngineShard::work_count).sum();
        let mut more = false;
        for s in shards.iter_mut() {
            more |= s.run_once(u64::MAX);
        }
        if !more && shards.iter().map(EngineShard::work_count).sum::<u64>() == work {
            return;
        }
    }
    panic!("the shards never went idle");
}

#[test]
fn a_flush_lets_every_checkpoint_pass_its_cross_records() {
    // One flush of everything: each checkpoint stops at most once per peer and is answered
    // once. A message per share or commit was 1200 (D254 sent 6).
    flush_passes(254, 600, 12, true);
}

/// Thousands of commits, all passed on the flush that put them in SSTs. Without tablet
/// changes no manifest commit makes a shard look at its checkpoint again (`Maintain` does
/// only with them on), so a lost wake would leave the checkpoints short.
#[test]
fn a_flush_lets_thousands_of_cross_records_pass_at_once() {
    flush_passes(777, 2100, 12, false);
}

/// The test's three tables: tablet n is on shard n % 3, so `c1` is on shard 1, `p` on
/// shard 2 and `c0` on shard 0.
fn tables(vfs: &Arc<SimVfs>, tablet_changes: bool) -> [Arc<TableInfo>; 3] {
    let db = Engine::open(Path::new(DB), options(vfs, tablet_changes)).unwrap();
    let family = || vec![("f".into(), FamilyOptions::default())];
    let c1 = db.create_table("c1", &family()).unwrap();
    let p = db.create_table("p", &family()).unwrap();
    let c0 = db.create_table("c0", &family()).unwrap();
    db.close().unwrap();
    [c0, c1, p]
}

fn put(wb: &mut WriteBatch, t: &TableInfo, row: &[u8]) {
    wb.put(
        t.id,
        t.families[0].id,
        row,
        b"q",
        None,
        ValueRef::Bytes(row),
    )
    .unwrap();
}

/// `commits` cross-shard commits over two coordinators and one participant, one flush of
/// everything, then: at most `max_msgs` `AwaitFlushed`/`FlushedThrough` messages, every
/// checkpoint past its records, and every commit kept by a power loss.
fn flush_passes(seed: u64, commits: u32, max_msgs: u64, tablet_changes: bool) {
    let vfs = SimVfs::new(seed);
    let [c0, c1, p] = tables(&vfs, tablet_changes);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, tablet_changes)).unwrap();
    // Each commit writes its coordinator's table first (the first row's shard coordinates),
    // then a row on shard 2: half are coordinated by shard 0, half by shard 1.
    for i in 0..commits {
        let row = i.to_be_bytes();
        let mut wb = WriteBatch::new();
        put(&mut wb, if i % 2 == 0 { &c0 } else { &c1 }, &row);
        put(&mut wb, &p, &row);
        drive(
            &mut shards,
            db.submit(wb, Some(Durability::Buffered)).unwrap(),
        )
        .unwrap();
    }
    // Shard 2 logged a PREPARE per commit, which its checkpoint can pass only once the
    // commit's coordinator has flushed its share too.
    assert_eq!(db.logged_records(2), u64::from(commits));
    let before: u64 = (0..3).map(|s| db.flushed_msgs(s)).sum();

    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);

    let msgs: Vec<u64> = (0..3).map(|s| db.flushed_msgs(s)).collect();
    let total: u64 = msgs.iter().sum::<u64>() - before;
    assert!(
        total <= max_msgs,
        "{total} AwaitFlushed/FlushedThrough messages for {commits} commits, per shard: {msgs:?}"
    );
    // Every commit's records passed: the participant's PREPAREs, and each coordinator's
    // PREPAREs and COMMITs.
    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(
        logged,
        vec![0; 3],
        "records the checkpoints have not passed"
    );

    // The commits survive a power loss whole.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, tablet_changes)).unwrap();
    for i in 0..commits {
        let row = i.to_be_bytes();
        let c = if i % 2 == 0 { &c0 } else { &c1 };
        assert_eq!(get(&db, c, &row).as_deref(), Some(&row[..]), "commit {i}");
        assert_eq!(get(&db, &p, &row).as_deref(), Some(&row[..]), "commit {i}");
    }
    db.close().unwrap();
}

fn get(db: &Engine, t: &TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    db.get_latest(t.id, FamilyId(t.families[0].id.0), row, b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

/// perf287's empty share: a transaction that reads a row on shard 2 and writes only on
/// shard 0 makes shard 2 a validation-only participant, with nothing to flush. Shard 2 never
/// flushes, so it raises its flushed-through seqno only when asked (`AwaitFlushed` finds
/// nothing unflushed and publishes): the coordinator's COMMIT and shard 2's PREPARE then
/// pass, and the commit survives a power loss.
#[test]
fn a_validation_only_share_on_an_idle_shard_lets_the_checkpoints_pass() {
    let vfs = SimVfs::new(2661);
    let [c0, _, p] = tables(&vfs, false);
    let (db, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs, false)).unwrap();
    let mut t = db.begin().unwrap();
    t.get(p.id, p.families[0].id, b"r", b"q").unwrap();
    put(t.batch(), &c0, b"w");
    let committed = drive(&mut shards, t.submit(Some(Durability::Buffered)).unwrap()).unwrap();
    settle(&mut shards);
    // Shard 2's PREPARE waits for shard 0's flush. Shard 0's checkpoint, stopped at its own
    // PREPARE, already asked shard 2, which had nothing to flush and answered by publishing.
    assert_eq!(db.logged_records(2), 1, "shard 2 logged no PREPARE");
    assert!(db.logged_records(0) >= 1);
    let f2 = db.flushed_through(2);
    assert!(
        f2 >= committed.seqno,
        "idle shard 2 published only {f2}, the commit is at {}",
        committed.seqno
    );

    let flush = db.submit_flush().unwrap();
    drive(&mut shards, flush).unwrap();
    settle(&mut shards);
    let logged: Vec<_> = (0..3).map(|s| db.logged_records(s)).collect();
    assert_eq!(
        logged,
        vec![0; 3],
        "records the checkpoints have not passed"
    );

    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let db = Engine::open(Path::new(DB), options(&vfs, false)).unwrap();
    assert_eq!(get(&db, &c0, b"w").as_deref(), Some(&b"w"[..]));
    db.close().unwrap();
}
