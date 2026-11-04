//! #1477: a running compaction pins its inputs' SST extents, never memtables. It took the
//! whole view for the job, and a view lists every shard's memtables, so every memtable
//! live when a compaction started stayed referenced until the compaction ended. Flushed,
//! their chunks couldn't be reused. Under a write load (an IVF memory store at 16 writers,
//! back-to-back compactions), 48-62 MiB of a 64 MiB arena sat in such memtables at room
//! waits. The compaction now keeps an SST-only view, as `maintenance.rs` phase 2 does.
//!
//! Application-owned shard on `SimVfs`: a compaction held before its first slice
//! (`hold_compactions`) took its view while a memtable was active; that memtable then
//! fills, flushes and retires, and its chunks must be free again while the compaction is
//! still held.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::SimVfs;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

const CHUNK: usize = 32 << 10;

#[test]
fn a_held_compaction_does_not_keep_a_flushed_memtables_chunks() {
    let vfs = SimVfs::new(1477);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    // Never due by size: this test decides every flush.
    o.memtable_freeze_bytes = 4 << 20;
    o.arena_chunk_bytes = Some(CHUNK);
    o.compaction.l0_trigger = 4;
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new("/db/compaction-view.phdb"), o).expect("open");
    let mut shard = shards.remove(0);
    let step = |shard: &mut pigeonhole_engine::EngineShard| {
        for _ in 0..64 {
            let now = vfs.monotonic_nanos();
            if !shard.run_once(now + 1_000_000) {
                break;
            }
        }
    };
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let commit = |shard: &mut pigeonhole_engine::EngineShard, row: &[u8], len: usize| {
        let mut b = WriteBatch::new();
        b.put(t.id, f, row, b"q", None, ValueRef::Bytes(&vec![0x5A; len]))
            .unwrap();
        let mut pc = engine.submit(b, Some(Durability::Buffered)).unwrap();
        for _ in 0..1000 {
            if let Some(r) = ready(&mut pc) {
                r.expect("commit");
                return;
            }
            step(shard);
        }
        panic!("a commit never finished");
    };
    let flush = |shard: &mut pigeonhole_engine::EngineShard| {
        let mut m = engine.flush_pending().unwrap();
        for _ in 0..1000 {
            if let Some(r) = ready(&mut m) {
                r.expect("flush");
                return;
            }
            step(shard);
        }
        panic!("a flush never finished");
    };
    // Four L0 runs; the fourth flush makes a compaction due, which the hook holds before its
    // first slice, its view taken while the slot's fresh active memtable is empty.
    engine.hold_compactions(true);
    for r in 0..4u8 {
        for i in 0..4u8 {
            commit(&mut shard, &[r, i], 1024);
        }
        flush(&mut shard);
    }
    step(&mut shard);
    assert!(engine.compaction_held(), "the compaction is held");
    let size = engine.arena_free(0).2;
    // That memtable now takes about 1.5 MiB, then flushes and retires.
    for i in 0..1536u32 {
        commit(&mut shard, format!("late{i:05}").as_bytes(), 1024);
    }
    let before = engine.arena_free(0).0;
    flush(&mut shard);
    step(&mut shard);
    let after = engine.arena_free(0).0;
    assert!(engine.compaction_held(), "the compaction is still held");
    // Its chunks are free again: only the fresh active memtable and a few chunks of
    // bookkeeping stay taken, not the 1.5 MiB the held compaction's view used to keep.
    assert!(
        after >= size - 8 * CHUNK as u64,
        "{after} of {size} bytes free after the flush ({before} before it): the held \
         compaction keeps the flushed memtable's chunks"
    );
    // The SST view is enough for the job: released, the compaction finishes (L0 merges),
    // and every row reads back, the late ones from the SST the retired memtable became.
    let l0 = || engine.sst_levels().iter().filter(|s| s.1 == 0).count();
    let l0_held = l0();
    engine.hold_compactions(false);
    for _ in 0..1000 {
        step(&mut shard);
        if l0() < l0_held {
            break;
        }
    }
    assert!(
        l0() < l0_held,
        "the released compaction never finished ({l0_held} L0 SSTs)"
    );
    let snap = engine.snapshot().unwrap();
    for r in 0..4u8 {
        for i in 0..4u8 {
            assert!(
                engine.get(&snap, t.id, f, &[r, i], b"q").unwrap().is_some(),
                "row {r}/{i}"
            );
        }
    }
    for i in [0u32, 767, 1535] {
        let row = format!("late{i:05}");
        assert!(
            engine
                .get(&snap, t.id, f, row.as_bytes(), b"q")
                .unwrap()
                .is_some(),
            "{row}"
        );
    }
    drop(snap);
    engine.close().unwrap();
    step(&mut shard);
}
