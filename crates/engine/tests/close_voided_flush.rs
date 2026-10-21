//! #838: a close whose in-place flushes (#111, a starved arena) ride in a flush that a voided
//! purge guard (#287) refuses. The refusal re-queued the frozen memtables only, so the
//! memtables flushed in place, sealed `Flushing`, were never flushed again and the close
//! waited for them for ever (nothing to flush, log records still needed). Found by the
//! public model check's recovery arm (#837) on CI; present since #315 (0.2.0).

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/voided.phdb";
const CHUNK: usize = 32 << 10;
/// Batches frozen behind the held flush, each with a guarded flush queued.
const ROUNDS: u8 = 3;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(f).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.arena_chunk_bytes = Some(CHUNK);
    o.memtable_freeze_bytes = CHUNK as u64;
    o
}

/// One application-owned shard on the simulator's frozen clock. A frozen memtable M has a
/// guarded flush queued behind a flush held at its manifest commit; a delete voids M's
/// guard; the close flushes the active memtable in place (no spare chunk) into the same
/// flush as M, which the manifest refuses for the voided guard. The close must still finish,
/// and a reopen reads every row.
#[test]
fn a_close_finishes_when_a_voided_guard_refuses_the_flush_of_its_in_place_memtables() {
    let vfs = SimVfs::new(838);
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs)).expect("open");
    let mut shard: EngineShard = shards.remove(0);
    let step = |shard: &mut EngineShard, turns: usize| {
        for _ in 0..turns {
            let now = pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
            if !shard.run_once(now + 1_000_000) {
                break;
            }
        }
    };
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("table");
    let f = t.families[0].id;
    let commit = |shard: &mut EngineShard, b: WriteBatch| {
        let mut pc: PendingCommit = engine.submit(b, Some(Durability::Buffered)).unwrap();
        step(shard, 32);
        ready(&mut pc).expect("committed").expect("ok");
    };
    let put = |row: &[u8]| {
        let mut b = WriteBatch::new();
        b.put(t.id, f, row, b"q", None, ValueRef::Bytes(b"v"))
            .unwrap();
        b
    };
    // M0: a few rows, frozen by a flush that stays in flight at its manifest commit.
    for i in 0..4u8 {
        commit(&mut shard, put(&[b'a', i]));
    }
    assert!(engine.hold_manifest());
    let mut first = engine.flush_pending().unwrap();
    step(&mut shard, 32);
    assert!(
        ready(&mut first).is_none(),
        "the first flush waits at the manifest"
    );
    // M, M1, …: more rows, each batch frozen by a flush whose item, with a purge guard
    // (no delete anywhere in the family), waits in the queue behind the first flush.
    let mut later = Vec::new();
    for r in 0..ROUNDS {
        for i in 0..4u8 {
            commit(&mut shard, put(&[b'b' + r, i]));
        }
        later.push(engine.flush_pending().unwrap());
        step(&mut shard, 32);
    }
    assert!(
        later.iter_mut().all(|f| ready(f).is_none()),
        "every later flush waits"
    );
    // A delete in the family voids M's guard; it lands in the active memtable.
    let mut b = WriteBatch::new();
    b.delete_row(t.id, &[b'a', 0], None).unwrap();
    commit(&mut shard, b);
    // The close: its freeze finds no chunk for a fresh memtable (a starved arena, by the
    // test hook: admission keeps a chunk back for the next freeze, so a small test cannot
    // starve it with commits) and flushes the active memtable in place, into the queue
    // behind M.
    engine.inject(pigeonhole_engine::Fault::NextFreezeStarved, true);
    engine.close().expect("close told the shard");
    step(&mut shard, 32);
    assert!(
        shard.closed().is_none(),
        "the close waits for the held flush"
    );
    // The held flush commits; the next one carries M and the in-place memtable, and the
    // manifest refuses it for M's voided guard. Before #838 the in-place memtable was then
    // dropped from the queue, and the close never reported.
    engine.release_manifest();
    step(&mut shard, 512);
    assert!(
        shard.closed().is_some(),
        "the close finished: the refused flush's in-place memtable was flushed again"
    );
    // The flush callers are answered by the close (its outcome, or `Closed`).
    assert!(ready(&mut first).is_some() || shard.closed().is_some());
    drop(later);
    drop(engine);
    drop(shard);
    // Every row survives: the two memtables' puts, and the delete.
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(&vfs)).expect("reopen");
    let mut shard = shards.remove(0);
    step(&mut shard, 32);
    let t = engine.table("t").expect("table");
    let snap = engine.snapshot().unwrap();
    for i in 0..4u8 {
        let a = engine
            .get(&snap, t.id, f, &[b'a', i], b"q")
            .unwrap()
            .is_some();
        assert_eq!(a, i != 0, "row a{i}");
        for r in 0..ROUNDS {
            assert!(
                engine
                    .get(&snap, t.id, f, &[b'b' + r, i], b"q")
                    .unwrap()
                    .is_some(),
                "row {}{i}",
                (b'b' + r) as char
            );
        }
    }
    drop(snap);
    engine.close().unwrap();
    step(&mut shard, 64);
}
