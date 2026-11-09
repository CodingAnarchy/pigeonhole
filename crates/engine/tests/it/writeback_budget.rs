//! D319 Part B (#1621): flush and compaction output is written back behind the job, under one
//! engine-wide budget of bytes in flight. A job that finds the budget full ends its slice
//! blocked and is woken by the write-backs completing: it never waits on its thread, and it
//! finishes once they drain. The close waits for every write-back in flight.
//!
//! The simulated disk defers I/O here, so the write-backs stay in flight until the test
//! completes them: the budget fills on the first flush that writes more than it.

use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    AppOwned, BackgroundWorker, Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::Vfs;
use pigeonhole_io::sim::SimVfs;

/// The engine's budget (`pigeonhole_sst::writeback::DEFAULT_BUDGET`).
const BUDGET: u64 = 4 << 20;
/// Values this long stay in the SST (below the default separation threshold).
const VALUE: usize = 16 << 10;

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(f).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

fn step(vfs: &dyn Vfs, shard: &mut EngineShard, worker: &mut BackgroundWorker, turns: usize) {
    for _ in 0..turns {
        let now = vfs.monotonic_nanos();
        shard.run_once(now + 1_000_000);
        worker.run_once(now + 1_000_000);
    }
}

fn put(t: &pigeonhole_engine::TableInfo, row: u32) -> WriteBatch {
    let f = t.families[0].id;
    let value = vec![row as u8; VALUE];
    let mut b = WriteBatch::new();
    b.put(
        t.id,
        f,
        &row.to_be_bytes(),
        b"q",
        None,
        ValueRef::Bytes(&value),
    )
    .unwrap();
    b
}

#[test]
fn a_flush_over_the_budget_waits_for_room_and_finishes_once_it_drains() {
    let vfs = SimVfs::new(11);
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.background_workers = 1;
    o.memtable_freeze_bytes = 64 << 20;
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(Path::new("/db/wb.phdb"), o).expect("open");
    let (shard, worker) = (&mut shards[0], &mut workers[0]);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // 12 MiB of values in one memtable: three budgets' worth of flush output.
    let rows = (12 << 20) / VALUE as u32;
    for r in 0..rows {
        let mut pc: PendingCommit = engine
            .submit(put(&t, r), Some(Durability::Buffered))
            .unwrap();
        step(&*vfs, shard, worker, 4);
        ready(&mut pc).expect("committed").expect("ok");
    }
    assert_eq!(
        engine.metrics().writeback_peak_bytes,
        0,
        "nothing flushed yet"
    );

    // Nothing completes while the flush writes: it fills the budget and stops.
    vfs.set_deferred_io(true);
    let mut flush = engine.flush_pending().unwrap();
    step(&*vfs, shard, worker, 256);
    assert!(ready(&mut flush).is_none(), "the flush waits for room");
    let m = engine.metrics();
    assert!(m.writeback_waits >= 1, "it found the budget full: {m:?}");
    assert!(
        m.writeback_peak_bytes >= BUDGET && m.writeback_peak_bytes <= BUDGET + (2 << 20),
        "at most the budget and a chunk in flight: {}",
        m.writeback_peak_bytes
    );
    // It waits blocked, not busy: more turns don't write more.
    let peak = m.writeback_peak_bytes;
    step(&*vfs, shard, worker, 256);
    assert_eq!(engine.metrics().writeback_peak_bytes, peak);

    // As the device catches up, the flush goes on and finishes.
    let mut done = None;
    for _ in 0..10_000 {
        vfs.complete_all_io();
        step(&*vfs, shard, worker, 4);
        if let Some(r) = ready(&mut flush) {
            done = Some(r);
            break;
        }
    }
    done.expect("the flush finished").expect("ok");
    let m = engine.metrics();
    assert!(m.flushes >= 1);
    assert!(
        m.writeback_peak_bytes <= BUDGET + (2 << 20),
        "{}",
        m.writeback_peak_bytes
    );
    vfs.complete_all_io();

    // Every row reads back.
    for r in [0, rows / 2, rows - 1] {
        let got = engine
            .get_latest(t.id, t.families[0].id, &r.to_be_bytes(), b"q")
            .unwrap()
            .expect("present");
        match got.value() {
            ValueRef::Bytes(b) => assert_eq!(b.len(), VALUE),
            other => panic!("{other:?}"),
        }
    }
    vfs.set_deferred_io(false);
    engine.close().unwrap();
    step(&*vfs, shard, worker, 256);
    assert!(shard.closed().expect("closed").is_ok());
}
