//! The writer-lock handover (D265, for #620's automatic recovery): `Engine::close_for_reopen`
//! closes an instance keeping its writer lock and presence, and `Engine::open_with` reopens
//! the same path on them, so no other writer gets in between and no reader's last-one-out
//! cleanup runs; `Engine::wait_failed` blocks until the instance fails.

mod gate;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    ArenaDiscard, Engine, EngineOptions, Error, FailureCause, FamilyOptions, ReadSpec, RowData,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::{OpenOptions, VfsRef};
use pigeonhole_shm::{Presence, WriterLock};

const DB: &str = "/db/h.phdb";

fn options(vfs: VfsRef, shards: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    o
}

fn wal(stream: u32) -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(stream))
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

fn has(db: &Engine, t: &TableInfo, row: &str) -> bool {
    let snap = db.snapshot().unwrap();
    db.get(&snap, t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .is_some()
}

/// Probes the file's lock bytes from a fresh handle: whether the writer byte is free, and
/// whether a presence taken now would be the last one out (no writer or reader present).
fn probe(vfs: &VfsRef) -> (bool, bool) {
    let mut opts = OpenOptions::read();
    opts.write = true;
    let file = vfs.open(Path::new(DB), opts).unwrap();
    let writer_free = match WriterLock::acquire(&file) {
        Ok(lock) => {
            drop(lock);
            true
        }
        Err(pigeonhole_shm::Error::WriterLocked) => false,
        Err(e) => panic!("{e}"),
    };
    let presence = Presence::acquire(&file).expect("the presence byte is shared");
    let last = presence.try_become_last().unwrap();
    drop(presence);
    (writer_free, last)
}

/// The shard owning `t`: commit to it and look at the stream the record went to.
fn shard_of(db: &Engine, t: &TableInfo) -> u32 {
    db.record_history(true);
    db.take_appended();
    db.commit(put(t, "probe"), Some(Durability::Buffered))
        .unwrap();
    u32::from(db.take_appended().last().expect("one record").stream)
}

/// A shard's WAL sync fails, the instance fails with `Io`, and the handover reopens it in
/// place: the writer byte is held throughout (a second writer is refused meanwhile), the
/// reopen replays what the WAL holds, and the new instance commits and closes cleanly.
#[test]
fn a_failed_instance_reopens_on_its_handover_with_the_writer_byte_held_throughout() {
    let (vfs, gate) = gate::vfs(1600);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s = shard_of(&db, &t);
    db.commit(put(&t, "before"), Some(Durability::Sync))
        .unwrap();
    gate.fail(&wal(s));
    let (db2, t2) = (Arc::clone(&db), Arc::clone(&t));
    let failed = gate::within(10, move || {
        db2.commit(put(&t2, "lost"), Some(Durability::Sync))
    })
    .expect("a commit with a failing sync hung");
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(db.failure().map(|f| f.cause), Some(FailureCause::Io));

    let db2 = Arc::clone(&db);
    let (closed, handover) =
        gate::within(10, move || db2.close_for_reopen()).expect("close_for_reopen hung");
    assert!(
        closed.is_err(),
        "a failed instance's close is unclean: {closed:?}"
    );
    let handover = handover.expect("the locks were still held");
    assert_eq!(probe(&vfs), (false, false), "held by the handover");
    // Another writer cannot get in: the handover holds the writer byte.
    match Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)) {
        Err(Error::WriterLocked) => {}
        other => panic!("a second writer opened during the handover: {other:?}"),
    }
    assert_eq!(
        probe(&vfs),
        (false, false),
        "still held after the refused open"
    );
    // The disk still fails: the reopen's replay sync fails, and the handover comes back with
    // the error, the locks still held, so the retry needs no plain open (D265's budget).
    let handover = match Engine::open_with(Path::new(DB), options(Arc::clone(&vfs), 2), handover) {
        Err((Error::Io(_), handover)) => handover,
        Ok(_) => panic!("the reopen succeeded on a failing disk"),
        Err((e, _)) => panic!("unexpected reopen error: {e:?}"),
    };
    assert_eq!(probe(&vfs), (false, false), "held across the failed reopen");
    // Another path is refused, and the handover comes back too.
    let handover = match Engine::open_with(
        Path::new("/db/other.phdb"),
        options(Arc::clone(&vfs), 2),
        handover,
    ) {
        Err((Error::InvalidArgument(_), handover)) => handover,
        other => panic!("another path was accepted: {:?}", other.map(|_| ())),
    };
    gate.heal(&wal(s));
    let reopened = Engine::open_with(Path::new(DB), options(Arc::clone(&vfs), 2), handover)
        .map_err(|(e, _)| e)
        .expect("reopen on the handover");
    assert_eq!(probe(&vfs), (false, false), "held by the reopened instance");
    assert!(!reopened.is_failed());
    let t = reopened.table("t").expect("the table survives");
    assert!(
        has(&reopened, &t, "probe"),
        "a Buffered commit before the failure"
    );
    assert!(
        has(&reopened, &t, "before"),
        "a Sync commit before the failure"
    );
    reopened
        .commit(put(&t, "after"), Some(Durability::Sync))
        .unwrap();
    assert!(has(&reopened, &t, "after"));
    drop(db);
    gate::within(10, move || reopened.close())
        .expect("close hung")
        .expect("the reopened instance closes cleanly");
    assert_eq!(probe(&vfs), (true, true), "released by the clean close");
    // The clean close released everything: a plain open works.
    let again = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = again.table("t").unwrap();
    assert!(has(&again, &t, "after"));
    again.close().unwrap();
}

/// A healthy instance's handover works too, and dropping a handover releases the locks as a
/// close would.
#[test]
fn a_dropped_handover_releases_the_locks() {
    let (vfs, _gate) = gate::vfs(1601);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.commit(put(&t, "row"), Some(Durability::Sync)).unwrap();
    let (closed, handover) = db.close_for_reopen();
    closed.expect("a healthy instance closes cleanly");
    let handover = handover.expect("the locks were still held");
    assert!(matches!(
        Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)),
        Err(Error::WriterLocked)
    ));
    assert_eq!(probe(&vfs), (false, false));
    drop(handover);
    assert_eq!(
        probe(&vfs),
        (true, true),
        "a dropped handover releases the writer byte and the presence"
    );
    let again = Engine::open(Path::new(DB), options(vfs, 2)).expect("open after the drop");
    let t = again.table("t").unwrap();
    assert!(has(&again, &t, "row"), "the clean close kept the data");
    again.close().unwrap();
}

/// A reader process attached before the handover: its old snapshot expires with the new
/// generation (D148), and a new snapshot reads what the reopened writer commits.
#[test]
fn a_reader_attached_before_the_handover_reads_the_new_generation() {
    let (vfs, _gate) = gate::vfs(1602);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.commit(put(&t, "old"), Some(Durability::Sync)).unwrap();
    let reader = Engine::open_reader(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let rt = reader.table("t").unwrap();
    let old_snap = reader.snapshot().unwrap();
    assert!(
        reader
            .get(&old_snap, rt.id, rt.families[0].id, b"old", b"q")
            .unwrap()
            .is_some()
    );

    let (closed, handover) = db.close_for_reopen();
    closed.unwrap();
    let reopened = Engine::open_with(
        Path::new(DB),
        options(Arc::clone(&vfs), 2),
        handover.unwrap(),
    )
    .map_err(|(e, _)| e)
    .unwrap();
    let t = reopened.table("t").unwrap();
    reopened
        .commit(put(&t, "new"), Some(Durability::Sync))
        .unwrap();

    assert!(matches!(
        reader.get(&old_snap, rt.id, rt.families[0].id, b"old", b"q"),
        Err(Error::SnapshotExpired)
    ));
    let snap = reader.snapshot().expect("re-attach to the new generation");
    for row in ["old", "new"] {
        assert!(
            reader
                .get(&snap, rt.id, rt.families[0].id, row.as_bytes(), b"q")
                .unwrap()
                .is_some(),
            "{row} through the new generation"
        );
    }
    drop((snap, old_snap));
    reader.close().unwrap();
    drop(db);
    reopened.close().unwrap();
}

/// `wait_failed` returns at a failure, at its deadline on a healthy instance, and once the
/// instance closed.
#[test]
fn wait_failed_returns_at_a_failure_at_the_deadline_and_at_the_close() {
    let (vfs, gate) = gate::vfs(1603);
    let db = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s = shard_of(&db, &t);
    let started = Instant::now();
    assert!(!db.wait_failed(started + Duration::from_millis(50)));
    assert!(started.elapsed() >= Duration::from_millis(50));

    let db2 = Arc::clone(&db);
    let waiter =
        std::thread::spawn(move || db2.wait_failed(Instant::now() + Duration::from_secs(30)));
    gate.fail(&wal(s));
    let (db2, t2) = (Arc::clone(&db), Arc::clone(&t));
    let _ = gate::within(10, move || {
        db2.commit(put(&t2, "lost"), Some(Durability::Sync))
    })
    .expect("a commit with a failing sync hung");
    let woke = gate::within(10, move || waiter.join().unwrap()).expect("wait_failed did not wake");
    assert!(woke, "a failure wakes the wait");
    assert!(
        db.wait_failed(Instant::now() + Duration::from_secs(30)),
        "already failed"
    );

    let db2 = Arc::clone(&db);
    let _ = gate::within(10, move || db2.close()).expect("close hung");
    // Closed: a wait on a healthy-looking question still ends.
    let (vfs2, _g) = gate::vfs(1604);
    let healthy = Engine::open(Path::new("/db/other.phdb"), options(vfs2, 1)).unwrap();
    let h2 = Arc::clone(&healthy);
    let waiter =
        std::thread::spawn(move || h2.wait_failed(Instant::now() + Duration::from_secs(30)));
    std::thread::sleep(Duration::from_millis(20));
    healthy.close().unwrap();
    let woke =
        gate::within(10, move || waiter.join().unwrap()).expect("the close did not end the wait");
    assert!(!woke, "a close ends the wait with false");
    // A reader handle: never fails, and its wait ends at its close too.
    let (vfs3, _g) = gate::vfs(1607);
    let writer = Engine::open(Path::new(DB), options(Arc::clone(&vfs3), 1)).unwrap();
    let reader = Engine::open_reader(Path::new(DB), options(vfs3, 1)).unwrap();
    let r2 = Arc::clone(&reader);
    let waiter =
        std::thread::spawn(move || r2.wait_failed(Instant::now() + Duration::from_secs(30)));
    std::thread::sleep(Duration::from_millis(20));
    reader.close().unwrap();
    let woke = gate::within(10, move || waiter.join().unwrap())
        .expect("the reader's close did not end the wait");
    assert!(!woke);
    writer.close().unwrap();
}

/// A `close_for_reopen` that finds a `close` under way does not take the locks from a live
/// instance (perf287's review of #788 reproduced the handover of a live instance's locks):
/// it waits for that close and reports no handover, and the close that started first
/// releases the locks when it ends.
#[test]
fn a_handover_asked_for_during_a_close_waits_for_it_and_gets_none() {
    let (vfs, gate) = gate::vfs(1608);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s = shard_of(&db, &t);
    db.commit(put(&t, "row"), Some(Durability::Buffered))
        .unwrap();
    // The close's final sync of shard `s` is held: the close is under way, not finished.
    gate.hold(&wal(s));
    let db2 = Arc::clone(&db);
    let closer = std::thread::spawn(move || db2.close());
    gate.wait_held(&wal(s));
    let db2 = Arc::clone(&db);
    let second = std::thread::spawn(move || db2.close_for_reopen());
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !closer.is_finished(),
        "the first close finished under a held sync"
    );
    assert!(
        !second.is_finished(),
        "close_for_reopen returned while the close that started first was still running"
    );
    assert_eq!(
        probe(&vfs),
        (false, false),
        "the live instance keeps its locks"
    );
    gate.release();
    gate::within(10, move || closer.join().unwrap())
        .expect("the first close hung")
        .expect("a clean close");
    let (result, handover) =
        gate::within(10, move || second.join().unwrap()).expect("close_for_reopen hung");
    assert!(result.is_ok(), "{result:?}");
    assert!(handover.is_none(), "a loser got the locks");
    assert_eq!(
        probe(&vfs),
        (true, true),
        "the first close released the locks"
    );
    let again = Engine::open(Path::new(DB), options(vfs, 2)).unwrap();
    let t = again.table("t").unwrap();
    assert!(has(&again, &t, "row"));
    again.close().unwrap();
}

/// Application-owned mode has no handover: the application drives the shards.
#[test]
fn an_application_owned_engine_refuses_the_handover() {
    let (vfs, _gate) = gate::vfs(1605);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs, 1)).unwrap();
    let (refused, handover) = db.close_for_reopen();
    assert!(
        matches!(refused, Err(Error::InvalidArgument(_))),
        "{refused:?}"
    );
    assert!(handover.is_none());
    db.close().unwrap();
    while shards[0].closed().is_none() {
        shards[0].run_once(u64::MAX);
    }
    shards[0].closed().unwrap().unwrap();
}

/// With every shard dead the final close may have run before `close_for_reopen`: then the
/// locks are already released and the caller falls back to a plain open. Either way the
/// database reopens with its data.
#[test]
fn a_handover_after_every_shard_died_reopens_either_way() {
    let (vfs, gate) = gate::vfs(1606);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.commit(put(&t, "kept"), Some(Durability::Sync)).unwrap();
    gate.panic(&wal(0));
    let (db2, t2) = (Arc::clone(&db), Arc::clone(&t));
    let _ = gate::within(10, move || {
        db2.commit(put(&t2, "boom"), Some(Durability::Sync))
    })
    .expect("a commit to the panicked shard hung");
    assert_eq!(db.failure().map(|f| f.cause), Some(FailureCause::Panic));
    let db2 = Arc::clone(&db);
    let (closed, handover) =
        gate::within(10, move || db2.close_for_reopen()).expect("close_for_reopen hung");
    assert!(closed.is_err(), "{closed:?}");
    let reopened = match handover {
        Some(h) => Engine::open_with(Path::new(DB), options(Arc::clone(&vfs), 1), h)
            .map_err(|(e, _)| e)
            .unwrap(),
        None => Engine::open(Path::new(DB), options(Arc::clone(&vfs), 1)).unwrap(),
    };
    let t = reopened.table("t").unwrap();
    assert!(has(&reopened, &t, "kept"));
    drop(db);
    reopened.close().unwrap();
}

/// Fails the instance through shard `s`'s WAL and closes it for a reopen: the handover.
fn fail_and_close(
    db: &Arc<Engine>,
    t: &Arc<TableInfo>,
    gate: &gate::Gate,
    s: u32,
) -> pigeonhole_engine::Handover {
    gate.fail(&wal(s));
    let (db2, t2) = (Arc::clone(db), Arc::clone(t));
    let failed = gate::within(10, move || {
        db2.commit(put(&t2, "lost"), Some(Durability::Sync))
    })
    .expect("a commit with a failing sync hung");
    assert!(failed.is_err(), "{failed:?}");
    let db2 = Arc::clone(db);
    let (closed, handover) =
        gate::within(10, move || db2.close_for_reopen()).expect("close_for_reopen hung");
    assert!(
        closed.is_err(),
        "a failed instance's close is unclean: {closed:?}"
    );
    handover.expect("the locks were still held")
}

/// A closed instance releases its memory (D265, #620 PR 2a): `close_for_reopen` publishes an
/// empty view and empties the caches; the arenas are freed once the reopen supersedes the
/// region and no view from before the close lives, which here is deferred to the drop of a
/// snapshot taken before the failure. On an open instance the release is a no-op.
#[test]
fn a_closed_instance_releases_its_memory_once_no_view_outlives_it() {
    let (vfs, gate) = gate::vfs(1606);
    let mut o = options(Arc::clone(&vfs), 2);
    o.row_cache_bytes = 1 << 20;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s = shard_of(&db, &t);
    for i in 0..64 {
        db.commit(put(&t, &format!("sst{i:03}")), Some(Durability::Buffered))
            .unwrap();
    }
    db.flush().unwrap();
    for i in 0..64 {
        db.commit(put(&t, &format!("mem{i:03}")), Some(Durability::Buffered))
            .unwrap();
    }
    // Reads fill the block cache and, read twice, the row cache (latest single-version row
    // reads fill it, D201).
    let mut spec = ReadSpec::default();
    spec.versions = 1;
    for _ in 0..2 {
        for i in 0..64 {
            let row = format!("sst{i:03}");
            let found = db
                .read_row_latest_into(
                    t.id,
                    row.as_bytes(),
                    &[t.families[0].id],
                    &spec,
                    &mut RowData::default(),
                )
                .unwrap();
            assert!(found, "{row}");
        }
    }
    let before = db.memory_release();
    assert!(
        before.view_memtables > 0
            && before.view_ssts > 0
            && before.block_cache_bytes > 0
            && before.row_cache_bytes.is_some_and(|b| b > 0),
        "{before:?} {:?}",
        db.row_cache_stats()
    );
    assert_eq!(before.arenas, ArenaDiscard::Pending);
    db.release_memory();
    assert_eq!(db.memory_release(), before, "a no-op on an open instance");

    let snap = db.snapshot().unwrap();
    let handover = fail_and_close(&db, &t, &gate, s);
    let closed = db.memory_release();
    assert_eq!(
        (
            closed.view_memtables,
            closed.view_ssts,
            closed.row_cache_bytes
        ),
        (0, 0, Some(0)),
        "{closed:?}"
    );
    assert!(
        closed.block_cache_bytes < before.block_cache_bytes,
        "the snapshot's SST set keeps its index blocks pinned; the rest went: {closed:?}"
    );
    assert_eq!(
        closed.arenas,
        ArenaDiscard::Pending,
        "the region is still current and a view lives"
    );
    // An engine snapshot from before the close keeps reading (D148: writer snapshots never
    // expire): its view keeps the memtables and SSTs it lists, and the arenas are not
    // discarded while it lives.
    let reads = |what: &str| {
        for row in ["sst000", "mem063"] {
            assert!(
                db.get(&snap, t.id, t.families[0].id, row.as_bytes(), b"q")
                    .unwrap()
                    .is_some(),
                "{row} through the old snapshot {what}"
            );
        }
    };
    reads("after the close");

    gate.heal(&wal(s));
    let reopened = Engine::open_with(Path::new(DB), options(Arc::clone(&vfs), 2), handover)
        .map_err(|(e, _)| e)
        .expect("reopen on the handover");
    db.release_memory();
    assert_eq!(
        db.memory_release().arenas,
        ArenaDiscard::Pending,
        "a snapshot from before the close still lives"
    );
    reads("after the reopen");
    drop(snap);
    let released = db.memory_release();
    assert_eq!(released.arenas, ArenaDiscard::Discarded, "{released:?}");
    assert_eq!(released.block_cache_bytes, 0, "{released:?}");

    let t2 = reopened.table("t").unwrap();
    for i in 0..64 {
        assert!(has(&reopened, &t2, &format!("sst{i:03}")));
        assert!(has(&reopened, &t2, &format!("mem{i:03}")));
    }
    reopened
        .commit(put(&t2, "after"), Some(Durability::Sync))
        .unwrap();
    assert!(has(&reopened, &t2, "after"));
    drop(db);
    gate::within(10, move || reopened.close())
        .expect("close hung")
        .expect("the reopened instance closes cleanly");
    assert_eq!(probe(&vfs), (true, true));
}

/// With no view outliving the close, the arenas are freed by the release the reopen is
/// followed by, and a plain `close` releases nothing (engine-level callers may still read
/// through their snapshots, D148).
#[test]
fn the_arenas_are_freed_by_the_release_after_the_reopen() {
    let (vfs, gate) = gate::vfs(1607);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let s = shard_of(&db, &t);
    for i in 0..32 {
        db.commit(put(&t, &format!("row{i:03}")), Some(Durability::Buffered))
            .unwrap();
    }
    let handover = fail_and_close(&db, &t, &gate, s);
    assert_eq!(db.memory_release().arenas, ArenaDiscard::Pending);
    gate.heal(&wal(s));
    let reopened = Engine::open_with(Path::new(DB), options(Arc::clone(&vfs), 2), handover)
        .map_err(|(e, _)| e)
        .unwrap();
    db.release_memory();
    let released = db.memory_release();
    assert_eq!(released.arenas, ArenaDiscard::Discarded, "{released:?}");
    assert_eq!((released.view_memtables, released.view_ssts), (0, 0));
    let t2 = reopened.table("t").unwrap();
    for i in 0..32 {
        assert!(has(&reopened, &t2, &format!("row{i:03}")));
    }
    drop(db);

    // A plain close keeps the view.
    let before = reopened.memory_release();
    assert!(before.view_memtables > 0, "{before:?}");
    gate::within(10, move || reopened.close())
        .expect("close hung")
        .unwrap();
}
