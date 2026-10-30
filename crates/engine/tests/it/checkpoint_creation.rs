//! D211 (#520): no manifest checkpoint may name a WAL stream whose file creation is not
//! durable. Since D203 (#454) an open submits the new stream files' directory sync without
//! waiting for it; a checkpoint committed before it completed survived a power loss that took
//! the files' names, and the reopened engine applied it to the stream it created afresh,
//! skipping the records an acknowledged commit had there. Deterministic: one shard driven by
//! hand, the simulator's I/O deferred and its directory syncs held.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    CommitInfo, Engine, EngineOptions, EngineShard, Error, FamilyOptions, PendingCommit, TableInfo,
    ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, StreamId};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::{CrashKind, SimVfs};

const DB: &str = "/db/data.phdb";

fn poll(pc: &mut PendingCommit) -> Poll<Result<CommitInfo, Error>> {
    Pin::new(pc).poll(&mut Context::from_waker(Waker::noop()))
}

fn put(t: &TableInfo, row: &str) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(&[7u8; 300]),
    )
    .unwrap();
    wb
}

fn open(vfs: &Arc<SimVfs>) -> (Arc<Engine>, Vec<EngineShard>) {
    let mut o = EngineOptions::new(Arc::clone(vfs) as VfsRef);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = 0;
    o.memtable_budget = 4 << 20;
    o.wal.segment_size = 256 << 10;
    o.wal.spare_segments = 1;
    Engine::open_application_owned(Path::new(DB), o).unwrap()
}

/// Runs the shard and completes the I/O the device may (held directory syncs stay in flight)
/// until neither has anything left to do.
fn settle(vfs: &SimVfs, shard: &mut EngineShard) {
    loop {
        while shard.run_once(u64::MAX) {}
        if !vfs.complete_io() && !shard.run_once(u64::MAX) {
            return;
        }
    }
}

/// Stream 0's checkpoint in the manifest on disk.
fn checkpoint(vfs: &Arc<SimVfs>) -> u64 {
    let vfs_ref: VfsRef = Arc::clone(vfs) as VfsRef;
    Engine::inspect_manifest(&vfs_ref, Path::new(DB))
        .unwrap()
        .checkpoints
        .get(&StreamId(0))
        .map_or(0, |l| l.0)
}

fn has(engine: &Engine, t: &TableInfo, row: &str) -> bool {
    engine
        .get_latest(t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .is_some()
}

#[test]
fn a_checkpoint_waits_for_its_streams_creation_to_be_durable() {
    let vfs = SimVfs::new(520);
    vfs.set_deferred_io(true);
    vfs.hold_dir_syncs(true);

    // A fresh database: its stream file is created, its directory sync held in flight. Rows
    // of a table then dropped need no flush to stop being needed: the checkpoint may pass
    // them at once (as it passes a refused attempt's PREPARE, #520's seed 273), with no WAL
    // sync behind the held directory sync.
    let (engine, mut shards) = open(&vfs);
    engine
        .create_table("t", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    let gone = engine
        .create_table("gone", &[("f".to_owned(), FamilyOptions::default())])
        .unwrap();
    settle(&vfs, &mut shards[0]);
    for i in 0..40 {
        drop(
            engine
                .submit(put(&gone, &format!("g{i:03}")), Some(Durability::Buffered))
                .unwrap(),
        );
    }
    settle(&vfs, &mut shards[0]);
    engine.drop_table(gone.id).unwrap();
    settle(&vfs, &mut shards[0]);
    // Nothing is needed in the stream, but its name is not durable: no checkpoint may name it.
    assert_eq!(
        checkpoint(&vfs),
        0,
        "a checkpoint was committed for a stream whose creation is not durable"
    );

    // A power loss before the directory sync: the stream file's name is gone.
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(engine);
    vfs.hold_dir_syncs(false);

    // The reopen creates the stream afresh; acknowledged Sync commits go into it.
    let (engine, mut shards) = open(&vfs);
    let t = engine.table("t").expect("the table survived");
    let mut acked = Vec::new();
    for i in 0..5 {
        let row = format!("b{i:03}");
        let mut pc = engine
            .submit(put(&t, &row), Some(Durability::Sync))
            .unwrap();
        for _ in 0..1000 {
            settle(&vfs, &mut shards[0]);
            if let Poll::Ready(r) = poll(&mut pc) {
                r.unwrap();
                acked.push(row);
                break;
            }
        }
    }
    assert_eq!(acked.len(), 5, "the Sync commits were acknowledged");
    vfs.crash(CrashKind::Power);
    drop(shards);
    drop(engine);

    // Every acknowledged commit survives, and so does the flushed data.
    let (engine, mut shards) = open(&vfs);
    settle(&vfs, &mut shards[0]);
    let t = engine.table("t").expect("the table survived");
    for row in &acked {
        assert!(has(&engine, &t, row), "acknowledged Sync commit {row} lost");
    }
}
