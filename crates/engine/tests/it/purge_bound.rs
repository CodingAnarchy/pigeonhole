//! #805: a bottommost compaction purges below `GcPolicy::min_ts_above`, the smallest
//! timestamp of the slot's sources outside its inputs (D70): its other SSTs and its
//! memtables. A memtable whose flush is already in the view is not such a source: its SST
//! is one of the inputs (or another SST of the slot, counted by its timestamp range). The
//! plan counted it anyway until the shard handled the flush's `Flushed`, so the compaction a
//! flush's new L0 SST triggers kept every version of every column that flush wrote, and
//! nothing compacted them again.

use crate::common;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Durability, Engine, EngineShard, FamilyOptions, Timestamp, ValueRef, WriteBatch,
};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/data.phdb";

fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

/// Runs the shards until they go idle: a pass in which no shard has work left or did any.
/// `run_once` says whether work remains after its turn, not whether it did any, so a pass is quiet
/// only if no shard did any work either (`EngineShard::work_count`): then it handled
/// nothing, sent nothing, and left nothing queued.
fn run(shards: &mut [EngineShard]) {
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

fn settle<F: Future + Unpin>(shards: &mut [EngineShard], f: &mut F) -> F::Output {
    for _ in 0..1_000 {
        if let Poll::Ready(r) = poll(f) {
            return r;
        }
        run(shards);
    }
    panic!("never settled");
}

#[test]
fn the_compaction_a_flush_triggers_purges_the_versions_that_flush_wrote() {
    let vfs = SimVfs::new(805);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.tablet_changes = false;
    o.compaction_threads = 0;
    // The fourth L0 SST triggers a compaction into L1, the only level then holding anything.
    o.compaction.l0_trigger = 4;
    o.compaction.level_base_bytes = u64::MAX;
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
    let t = db
        .create_table(
            "t",
            &[("f".into(), FamilyOptions::default().max_versions(1))],
        )
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    db.record_history(true);
    // Four versions of one column, each flushed to an SST of its own.
    for ts in [10, 20, 30, 40] {
        let mut wb = WriteBatch::new();
        wb.put(tid, fid, b"r", b"q", Some(ts), ValueRef::Bytes(b"v"))
            .unwrap();
        let mut pc = db.submit(wb, Some(Durability::Buffered)).unwrap();
        settle(&mut shards, &mut pc).unwrap();
        let mut flush = db.flush_pending().unwrap();
        settle(&mut shards, &mut flush).unwrap();
    }
    run(&mut shards);
    // The triggered compaction is bottommost, and nothing outside its inputs holds an entry:
    // the memtable of the last flush is in them.
    let compactions = db.take_compactions();
    assert_eq!(compactions.len(), 1, "{compactions:?}");
    assert!(compactions[0].bottommost);
    assert_eq!(
        compactions[0].min_ts_above,
        Timestamp::MAX,
        "{compactions:?}"
    );
    let snap = db.snapshot().unwrap();
    let versions = db
        .raw_entries(&snap)
        .unwrap()
        .into_iter()
        .filter(|e| e.table == tid && e.family == fid)
        .count();
    drop(snap);
    assert_eq!(versions, 1, "max_versions(1) over 4 versions of one column");
    db.close().unwrap();
    run(&mut shards);
}
