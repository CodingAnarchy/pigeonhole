//! #847: a flush whose shard's WAL stream is poisoned. Its sync barrier on the poisoned
//! stream must not pass for the shard's own flush unless the stream is durable through
//! everything appended: a `None`-durability commit applied to the memtables but never
//! written to the log (its buffered record goes with the failed write of the next group)
//! otherwise becomes durable in the slots that flush and not in the others, and a reopen
//! reads a part of it. Found by the public model check's recovery arm (#837) on the Linux
//! VM: seeds 401 and 496, a cell of such a commit survived a recovery while no allowed
//! prefix matched.

mod gate;

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit, ValueRef, WriteBatch,
};
use pigeonhole_format::{Durability, StreamId};
use pigeonhole_io::VfsRef;

const DB: &str = "/db/barrier.phdb";

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(f).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

fn options(vfs: VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.memtable_freeze_bytes = 512 << 10;
    o
}

/// One application-owned shard on the gate VFS with the simulator's frozen clock, driven a
/// turn at a time. A turn whose deadline has passed handles the queued messages and runs no
/// background task, so the flush can be made to run after the failing group:
/// 1. a `None` commit writes 768 KiB to `f1` (its memtable freezes on size right after the
///    group and a flush is queued, not yet run) and one cell to `f2`;
/// 2. the next commit's WAL write fails: the shard poisons itself, and nothing of that
///    group (the buffered `None` record included) reaches the file;
/// 3. a turn with time runs the flush: it writes its SST and asks the poisoned shard for
///    its barrier;
/// 4. the close's flush of `f2` fails (the data file refuses writes), so the close is unclean.
///
/// After a reopen, the `None` commit is there whole or not at all. With the barrier
/// answering `Ok` on the poisoned stream, the 96 cells of `f1` were there and the cell of
/// `f2` was not: the SST and its `SetFlushed` had made a record durable in part that the
/// log never held.
#[test]
fn a_flush_on_a_poisoned_stream_does_not_make_an_unlogged_commit_durable_in_part() {
    let (vfs, gate) = gate::frozen_vfs(847);
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs))).unwrap();
    let mut shard: EngineShard = shards.remove(0);
    let now = || pigeonhole_io::Vfs::monotonic_nanos(&*vfs);
    // Messages only: the deadline has passed, so no background task runs.
    let messages = |shard: &mut EngineShard| {
        for _ in 0..8 {
            shard.run_once(now());
        }
    };
    // Messages and tasks.
    let turns = |shard: &mut EngineShard, n: usize| {
        for _ in 0..n {
            if !shard.run_once(now() + 1_000_000) {
                break;
            }
        }
    };
    let t = engine
        .create_table(
            "t",
            &[
                ("f1".into(), FamilyOptions::default()),
                ("f2".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    let (f1, f2) = (t.families[0].id, t.families[1].id);
    // 1. The None commit: 768 KiB in f1 (three arena chunks, over the freeze threshold), one cell in f2.
    let mut b = WriteBatch::new();
    for i in 0..96u8 {
        b.put(
            t.id,
            f1,
            &[b'r', i],
            b"q",
            None,
            ValueRef::Bytes(&[7u8; 8 << 10]),
        )
        .unwrap();
    }
    b.put(t.id, f2, b"row", b"q", None, ValueRef::Bytes(b"small"))
        .unwrap();
    let mut a: PendingCommit = engine.submit(b, Some(Durability::None)).unwrap();
    messages(&mut shard);
    ready(&mut a).expect("the None commit was applied").unwrap();
    assert_eq!(engine.metrics().flushes, 0, "the flush is queued, not run");
    // 2. The next group's write fails: the shard poisons itself.
    let wal = pigeonhole_wal::stream_path(Path::new(DB), StreamId(0));
    gate.fail_writes(&wal);
    let mut b = WriteBatch::new();
    b.put(t.id, f2, b"row2", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    let mut bc = engine.submit(b, Some(Durability::Buffered)).unwrap();
    messages(&mut shard);
    let e = ready(&mut bc).expect("the commit was answered");
    assert!(e.is_err(), "the commit's log write failed: {e:?}");
    // (0.2.x has no `Engine::is_failed`, ICR 0040: the failed commit is the evidence.)
    // 3. The flush of f1 runs now: its barrier finds the stream poisoned and refuses, since
    //    the stream is not durable through the record the memtable holds.
    turns(&mut shard, 64);
    let m = engine.metrics();
    eprintln!(
        "after the flush: flushes {} failures {}",
        m.flushes, m.flush_failures
    );
    assert_eq!(m.flushes, 0, "no flush landed on the poisoned stream");
    assert_eq!(m.flush_failures, 1, "the barrier refused the flush");
    // 4. The close's flush of f2 fails: the close gives up, unclean.
    gate.fail_writes(Path::new(DB));
    engine.close().expect("close told the shard");
    turns(&mut shard, 256);
    let closed = shard.closed().expect("the close finished");
    assert!(
        closed.is_err(),
        "a poisoned shard's close is unclean: {closed:?}"
    );
    drop(engine);
    drop(shard);
    gate.heal_writes(&wal);
    gate.heal_writes(Path::new(DB));
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new(DB), options(Arc::clone(&vfs))).unwrap();
    let mut shard = shards.remove(0);
    turns(&mut shard, 16);
    let t = engine.table("t").unwrap();
    let snap = engine.snapshot().unwrap();
    let in_f1 = (0..96u8)
        .filter(|&i| {
            engine
                .get(&snap, t.id, f1, &[b'r', i], b"q")
                .unwrap()
                .is_some()
        })
        .count();
    let in_f2 = engine.get(&snap, t.id, f2, b"row", b"q").unwrap().is_some();
    eprintln!("after the reopen: {in_f1} of 96 f1 cells, f2 {in_f2}");
    assert!(
        (in_f1 == 96 && in_f2) || (in_f1 == 0 && !in_f2),
        "the None commit survived in part: {in_f1} of 96 f1 cells, f2 {in_f2}"
    );
    drop(snap);
    engine.close().unwrap();
    turns(&mut shard, 64);
}
