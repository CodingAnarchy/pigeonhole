//! #1403: a group the shard holds back for its WAL stream (#19) resumes once the stream can
//! take it, whichever of the stream's two waits ends last. The stream is blocked while its
//! segment is nearly full and either its header still waits for the previous rollover's
//! sync or no spare slot is ready. Here the header's wait ends first, while the spare's
//! preparation is still held, so the shard is still held back after the sync's wake; the
//! spare landing later must kick it again. Before the fix the shard registered its wake
//! once per hold (a latch), on the header's sync alone, so nothing kicked it when the spare
//! landed and a lone writer waited for ever (a bulk `remember` on the AX102).
//!
//! Deterministic: one shard driven by hand, the gate VFS holding the stream's syncs (so the
//! rollover's header waits) and its submitted writes (the next spare's zero-fill), released
//! in that order.

use crate::gate;

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::VfsRef;

const DB: &str = "/db/hold.phdb";

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.compaction_threads = 0;
    // No flush, so no checkpoint frees a slot to recycle: the next segment needs a spare.
    o.memtable_budget = 16 << 20;
    o.tablet_changes = false;
    // Small segments, so a few commits roll the stream over.
    o.wal.segment_size = 4 * 32 * 1024;
    o.wal.spare_segments = 1;
    o
}

fn wal() -> PathBuf {
    pigeonhole_wal::stream_path(Path::new(DB), pigeonhole_format::StreamId(0))
}

fn put(t: &TableInfo, i: u32, value: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        format!("row{i:04}").as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(value),
    )
    .unwrap();
    wb
}

fn run(shard: &mut EngineShard, turns: usize) {
    for _ in 0..turns {
        shard.run_once(u64::MAX);
    }
}

/// Whether every commit in `pending` has answered; each must have succeeded.
fn all_answered(pending: &mut [Option<PendingCommit>]) -> bool {
    let mut cx = Context::from_waker(Waker::noop());
    for slot in pending.iter_mut() {
        if let Some(pc) = slot.as_mut() {
            match Pin::new(pc).poll(&mut cx) {
                Poll::Ready(r) => {
                    r.unwrap();
                    *slot = None;
                }
                Poll::Pending => {}
            }
        }
    }
    pending.iter().all(Option::is_none)
}

#[test]
fn a_group_held_for_the_wal_resumes_when_the_spare_lands_after_the_header_sync() {
    let (vfs, gate) = gate::vfs(1403);
    let (db, mut shards) = Engine::open_application_owned(Path::new(DB), options(vfs)).unwrap();
    let shard = &mut shards[0];
    let t = (*db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap())
    .clone();
    run(shard, 100);
    let big = vec![7u8; 20_000];
    let mut i = 0u32;

    // Over a segment with nothing held: the stream rolls over once and prepares the next
    // spare (due once it has left the segment it opened in), so the next rollover takes a
    // prepared slot and its header can wait behind the full segment's sync.
    let mut pending = Vec::new();
    for _ in 0..10 {
        pending.push(Some(
            db.submit(put(&t, i, &big), Some(Durability::Buffered))
                .unwrap(),
        ));
        i += 1;
        run(shard, 50);
    }
    for _ in 0..1000 {
        if all_answered(&mut pending) {
            break;
        }
        run(shard, 10);
    }
    assert!(all_answered(&mut pending), "the unheld commits answered");

    // Hold the stream's syncs and submitted writes, then fill on: the next rollover takes
    // the prepared spare and its header waits for the full segment's sync, and the spare
    // preparation that follows stays held at its zero-fill.
    gate.hold(&wal());
    gate.hold_writes(&wal());
    let blocks_before = db.metrics().wal_rollover_blocks;
    let mut pending = Vec::new();
    loop {
        pending.push(Some(
            db.submit(put(&t, i, &big), Some(Durability::Buffered))
                .unwrap(),
        ));
        i += 1;
        run(shard, 50);
        // Filled the new segment to its last quarter while its header waits: the shard holds
        // the next group back and registers its wake on the header's sync.
        if db.metrics().wal_rollover_blocks > blocks_before {
            break;
        }
        assert!(
            i < 200,
            "the stream never held a group back behind its header"
        );
    }
    assert!(
        shard.wal_header_held(),
        "the segment's header still waits for the predecessor's sync"
    );
    // One more commit behind the held-back group: the lone writer's next commit.
    pending.push(Some(
        db.submit(put(&t, i, b"last"), Some(Durability::Buffered))
            .unwrap(),
    ));
    run(shard, 50);
    assert!(
        !all_answered(&mut pending),
        "a group is held back while the header waits: the test is not vacuous"
    );

    // The header's sync lands first. The wake it carried kicks the shard, which finds the
    // stream still blocked: no spare is ready, its zero-fill is held.
    gate.release();
    run(shard, 200);
    assert!(
        !shard.wal_header_held(),
        "the header's wait ended with its predecessor's sync"
    );

    // Then the spare lands. Nothing else will kick this shard: one writer, every commit
    // waiting on it.
    gate.release_writes();
    for _ in 0..2000 {
        if all_answered(&mut pending) {
            break;
        }
        run(shard, 10);
    }
    assert!(
        all_answered(&mut pending),
        "a group held back for the WAL was never resumed after the spare landed (#1403)"
    );
}
