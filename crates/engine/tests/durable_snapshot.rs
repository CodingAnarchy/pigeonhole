//! Durable snapshots (D227, ICR 0037): a snapshot sees a commit once it is applied, before
//! its sync returns; a durable snapshot sees it only once it is durable at its level, in the
//! writer and in reader processes. A memtable freezes only once everything in it is durable,
//! so flush and compaction never collapse a version a durable snapshot may still read.
//! Deterministic: shards driven by hand, the simulator's I/O deferred until the test
//! completes it.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineShard, FamilyOptions, Snapshot, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::ProcessId;
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";

fn put(t: &TableInfo, row: &[u8], v: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(t.id, t.families[0].id, row, b"q", None, ValueRef::Bytes(v))
        .unwrap();
    wb
}

fn idle(shards: &mut [EngineShard]) {
    for s in shards.iter_mut() {
        while s.run_once(u64::MAX) {}
    }
}

fn read(db: &Engine, snap: &Snapshot, t: &TableInfo, row: &[u8]) -> Option<Vec<u8>> {
    db.get(snap, t.id, t.families[0].id, row, b"q")
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

/// Two shards, one table on each, everything flushed and idle, then deferred I/O.
fn rig(seed: u64) -> (Arc<SimVfs>, Arc<Engine>, Vec<EngineShard>, [TableInfo; 2]) {
    let vfs = SimVfs::new(seed);
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });
    let mut o = common::options(Arc::clone(&vfs), 2, 4 << 20);
    // Each table stays one tablet, on shard `tablet % 2`.
    o.tablet_changes = false;
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let fam = || vec![("f".to_owned(), FamilyOptions::default())];
    let a = (*db.create_table("a", &fam()).unwrap()).clone();
    let b = (*db.create_table("b", &fam()).unwrap()).clone();
    idle(&mut shards);
    vfs.set_deferred_io(true);
    (vfs, db, shards, [a, b])
}

#[test]
fn a_durable_snapshot_sees_a_commit_only_once_its_sync_returned() {
    let (vfs, db, mut shards, [a, b]) = rig(227);
    // A GroupSync commit on one shard: applied, its sync in flight.
    let mut held = db
        .submit(put(&b, b"x", b"1"), Some(Durability::GroupSync))
        .unwrap();
    idle(&mut shards);
    assert!(vfs.io_in_flight() > 0, "the sync is in flight");
    // A later Buffered commit on the other shard resolves at once.
    let mut later = db
        .submit(put(&a, b"y", b"2"), Some(Durability::Buffered))
        .unwrap();
    idle(&mut shards);

    let visible = db.snapshot().unwrap();
    assert_eq!(read(&db, &visible, &b, b"x").as_deref(), Some(&b"1"[..]));
    assert_eq!(read(&db, &visible, &a, b"y").as_deref(), Some(&b"2"[..]));
    let durable = db.durable_snapshot().unwrap();
    assert!(durable.floor() < visible.floor());
    assert_eq!(read(&db, &durable, &b, b"x"), None, "unsynced commit seen");
    // A later commit above the unsynced one is not in the durable view either: it holds
    // every seqno at or below it.
    assert_eq!(read(&db, &durable, &a, b"y"), None);

    // A reader process sees the same two views through shared memory.
    vfs.enter_process(ProcessId {
        pid: 2,
        start_time: 1,
    });
    let reader =
        Engine::open_reader(Path::new(DB), common::options(Arc::clone(&vfs), 2, 4 << 20)).unwrap();
    let rb = (*reader.table("b").unwrap()).clone();
    let r_visible = reader.snapshot().unwrap();
    assert_eq!(
        read(&reader, &r_visible, &rb, b"x").as_deref(),
        Some(&b"1"[..])
    );
    let r_durable = reader.durable_snapshot().unwrap();
    assert_eq!(read(&reader, &r_durable, &rb, b"x"), None);
    drop((r_visible, r_durable));
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });

    // The sync returns: the durable view catches up.
    vfs.complete_all_io();
    idle(&mut shards);
    let durable = db.durable_snapshot().unwrap();
    assert_eq!(read(&db, &durable, &b, b"x").as_deref(), Some(&b"1"[..]));
    assert_eq!(read(&db, &durable, &a, b"y").as_deref(), Some(&b"2"[..]));
    vfs.enter_process(ProcessId {
        pid: 2,
        start_time: 1,
    });
    let r_durable = reader.durable_snapshot().unwrap();
    assert_eq!(
        read(&reader, &r_durable, &rb, b"x").as_deref(),
        Some(&b"1"[..])
    );
    drop(r_durable);
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });
    let poll = |f: &mut pigeonhole_engine::PendingCommit| common::poll_commit(f).is_ready();
    assert!(poll(&mut held) && poll(&mut later));
}

#[test]
fn a_memtable_freezes_only_once_everything_in_it_is_durable() {
    let (vfs, db, mut shards, [a, b]) = rig(2271);
    // Versions on shard 0, then an unsynced commit on shard 1 that holds the durable seqno
    // below a newer version on shard 0.
    for v in [b"1", b"2"] {
        drop(
            db.submit(put(&a, b"k", v), Some(Durability::Buffered))
                .unwrap(),
        );
    }
    idle(&mut shards);
    drop(
        db.submit(put(&b, b"x", b"held"), Some(Durability::GroupSync))
            .unwrap(),
    );
    idle(&mut shards);
    drop(
        db.submit(put(&a, b"k", b"3"), Some(Durability::Buffered))
            .unwrap(),
    );
    idle(&mut shards);
    let durable = db.durable_snapshot().unwrap();
    let before = db.metrics().flushes;
    // A flush now would write shard 0's memtable, newest version only where no snapshot
    // needs more: its freeze waits until the durable seqno covers the memtable.
    let mut flush = db.flush_pending().unwrap();
    idle(&mut shards);
    assert_eq!(
        db.metrics().flushes,
        before,
        "froze above the durable seqno"
    );
    assert_eq!(read(&db, &durable, &a, b"k").as_deref(), Some(&b"2"[..]));
    drop(durable);
    vfs.complete_all_io();
    idle(&mut shards);
    for _ in 0..8 {
        vfs.complete_all_io();
        idle(&mut shards);
    }
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(
        std::pin::Pin::new(&mut flush).poll(&mut cx).is_ready(),
        "the flush finished"
    );
    assert!(db.metrics().flushes > before);
    let durable = db.durable_snapshot().unwrap();
    assert_eq!(read(&db, &durable, &a, b"k").as_deref(), Some(&b"3"[..]));
}

/// Records recovered after a process crash may be in the page cache only: written, never
/// synced (a `Buffered` commit). Open syncs each recovered stream before any watermark is
/// published (`Recovery::into_stream`: the new segment's header sync, and the slot syncs
/// before it, cover the file), so a durable snapshot taken after the reopen, even before any
/// shard turn, reads only what survives a power loss that follows. Without the reopen the
/// same power loss removes the commit.
#[test]
fn a_durable_snapshot_after_a_process_crash_reads_only_what_a_power_loss_keeps() {
    let (vfs, db, mut shards, [_, b]) = rig(229);
    // Written to the OS and never synced (Buffered) when the process dies.
    let committed = db
        .submit(put(&b, b"x", b"1"), Some(Durability::Buffered))
        .unwrap();
    idle(&mut shards);
    drop(committed);
    vfs.crash(CrashKind::Process);
    drop(shards);
    drop(db);
    // The dead process's sync never runs: the record is in the page cache only.
    vfs.set_deferred_io(false);

    let reopen = |vfs: &Arc<SimVfs>| {
        let mut o = common::options(Arc::clone(vfs), 2, 4 << 20);
        o.tablet_changes = false;
        Engine::open_application_owned(Path::new(DB), o).unwrap()
    };
    // Before any shard turn: no later sync of the stream (a spare slot's) has run yet.
    let (db, shards) = reopen(&vfs);
    let snap = db.durable_snapshot().unwrap();
    let seen = read(&db, &snap, &b, b"x");
    assert_eq!(
        seen.as_deref(),
        Some(&b"1"[..]),
        "the recovered commit is replayed"
    );
    drop(snap);
    // A power loss now keeps whatever that durable snapshot read.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(db);
    let (db, mut shards) = reopen(&vfs);
    idle(&mut shards);
    let snap = db.snapshot().unwrap();
    assert_eq!(
        read(&db, &snap, &b, b"x").as_deref(),
        Some(&b"1"[..]),
        "a durable snapshot read a recovered record a power loss then removed"
    );
}
