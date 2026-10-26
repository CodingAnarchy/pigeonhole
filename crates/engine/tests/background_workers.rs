//! Application-owned background workers (D284): flush and compaction run on workers the
//! application drives, and nothing the application stops driving keeps the engine from
//! closing or a shard from getting room. A worker's slices run their I/O inline, so a task
//! it held resumes anywhere, under the pread and io_uring backends alike
//! (`PIGEONHOLE_IO=uring` picks io_uring on Linux, as `handover_real.rs` does).

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    AppOwned, BackgroundWorker, Engine, EngineOptions, EngineShard, FamilyOptions, PendingCommit,
    ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{Vfs, VfsRef};

fn ready<F: std::future::Future + Unpin>(f: &mut F) -> Option<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(f).poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

/// `turns` turns of the shard (and, if given, the worker): a fixed count, since one may
/// wait on a message the other has not sent yet.
fn step(
    vfs: &dyn Vfs,
    shard: &mut EngineShard,
    mut worker: Option<&mut BackgroundWorker>,
    turns: usize,
) {
    for _ in 0..turns {
        let now = vfs.monotonic_nanos();
        shard.run_once(now + 1_000_000);
        if let Some(w) = worker.as_deref_mut() {
            w.run_once(now + 1_000_000);
        }
    }
}

const CHUNK: usize = 32 << 10;

fn sim_options(vfs: &Arc<SimVfs>, workers: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs.clone());
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.arena_chunk_bytes = Some(CHUNK);
    o.memtable_freeze_bytes = CHUNK as u64;
    o.background_workers = workers;
    o
}

fn put(t: &pigeonhole_engine::TableInfo, row: &[u8], len: usize) -> WriteBatch {
    let f = t.families[0].id;
    let value = vec![row.last().copied().unwrap_or(0); len];
    let mut b = WriteBatch::new();
    b.put(t.id, f, row, b"q", None, ValueRef::Bytes(&value))
        .unwrap();
    b
}

#[test]
fn the_old_open_refuses_workers_it_would_not_hand_out() {
    let vfs = SimVfs::new(1);
    let err = Engine::open_application_owned(Path::new("/db/w.phdb"), sim_options(&vfs, 1))
        .expect_err("refused");
    assert!(
        err.to_string().contains("open_application_owned_with"),
        "{err}"
    );
}

#[test]
fn flushes_run_on_the_worker_not_the_shard() {
    let vfs = SimVfs::new(2);
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(Path::new("/db/w.phdb"), sim_options(&vfs, 1))
        .expect("open");
    let (shard, worker) = (&mut shards[0], &mut workers[0]);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for i in 0..8u8 {
        let mut pc: PendingCommit = engine
            .submit(put(&t, &[b'a', i], 64), Some(Durability::Buffered))
            .unwrap();
        step(&*vfs, shard, None, 32);
        ready(&mut pc).expect("committed").expect("ok");
    }
    let mut flush = engine.flush_pending().unwrap();
    step(&*vfs, shard, None, 64);
    assert!(
        ready(&mut flush).is_none(),
        "the shard alone does not flush"
    );
    assert_eq!(engine.metrics().flushes, 0);
    step(&*vfs, shard, Some(worker), 64);
    ready(&mut flush).expect("flushed").expect("ok");
    assert!(engine.metrics().flushes >= 1);
    assert_eq!(engine.metrics().background_take_backs, (0, 0));
    engine.close().unwrap();
    step(&*vfs, shard, Some(worker), 256);
    assert!(shard.closed().expect("closed").is_ok());
    assert!(worker.closed());
}

/// The owner's case (D284): a worker takes a flush and a compaction and runs part of each
/// (the flush wrote its output and waits at its manifest commit; the compaction is held
/// in its first slice), then the application stops driving it, without dropping it. The
/// close completes with only the shard driven: the flush finishes on the shard, the
/// compaction is dropped, and every row survives a reopen.
#[test]
fn close_completes_with_a_stopped_worker_holding_a_flush_and_a_compaction() {
    const DB: &str = "/db/stopped.phdb";
    let vfs = SimVfs::new(3);
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(Path::new(DB), sim_options(&vfs, 1)).expect("open");
    let mut shard = shards.remove(0);
    let mut worker = workers.remove(0);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let commit = |shard: &mut EngineShard, worker: &mut BackgroundWorker, b: WriteBatch| {
        let mut pc: PendingCommit = engine.submit(b, Some(Durability::Buffered)).unwrap();
        step(&*vfs, shard, Some(worker), 32);
        ready(&mut pc).expect("committed").expect("ok");
    };
    // Four L0 runs, the fourth starting a compaction the hook holds in its first slice.
    engine.hold_compactions(true);
    for r in 0..4u8 {
        for i in 0..4u8 {
            commit(&mut shard, &mut worker, put(&t, &[r, i], 64));
        }
        let mut f = engine.flush_pending().unwrap();
        step(&*vfs, &mut shard, Some(&mut worker), 64);
        ready(&mut f).expect("flushed").expect("ok");
    }
    step(&*vfs, &mut shard, Some(&mut worker), 64);
    assert!(engine.compaction_held(), "the worker holds the compaction");
    // A fifth flush writes its output on the worker and waits at its manifest commit.
    assert!(engine.hold_manifest());
    for i in 0..4u8 {
        commit(&mut shard, &mut worker, put(&t, &[9, i], 64));
    }
    let mut held = engine.flush_pending().unwrap();
    step(&*vfs, &mut shard, Some(&mut worker), 64);
    assert!(ready(&mut held).is_none(), "the flush waits at its commit");
    let flushes = engine.metrics().flushes;
    // The application stops driving the worker (it keeps it). The close, from another
    // thread, never waits for it.
    let closer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || engine.close())
    };
    // The close takes the worker's tasks back first.
    while !worker.closed() {
        std::thread::yield_now();
    }
    engine.release_manifest();
    while shard.closed().is_none() {
        step(&*vfs, &mut shard, None, 64);
        std::thread::yield_now();
    }
    assert!(shard.closed().unwrap().is_ok(), "a clean close");
    closer.join().unwrap().expect("close");
    assert!(worker.closed());
    assert!(!worker.run_once(u64::MAX), "nothing left to run");
    let m = engine.metrics();
    assert!(m.flushes > flushes, "the held flush finished on the shard");
    assert_eq!(
        m.compactions_reclaimed, 1,
        "the held compaction was dropped"
    );
    assert_eq!(m.compaction_failures, 0);
    drop(held);
    drop((engine, shard, worker));
    // Every row survives.
    let (engine, mut shards) =
        Engine::open_application_owned(Path::new(DB), sim_options(&vfs, 0)).expect("reopen");
    let mut shard = shards.remove(0);
    step(&*vfs, &mut shard, None, 32);
    let t = engine.table("t").unwrap();
    let f = t.families[0].id;
    let snap = engine.snapshot().unwrap();
    for r in (0..4u8).chain([9]) {
        for i in 0..4u8 {
            assert!(
                engine.get(&snap, t.id, f, &[r, i], b"q").unwrap().is_some(),
                "row {r}/{i}"
            );
        }
    }
    drop(snap);
    assert_eq!(engine.unreferenced_bytes(), 0);
    engine.close().unwrap();
    step(&*vfs, &mut shard, None, 64);
}

/// A wait for memtable room on a shard whose worker is not driven takes the flush back
/// after 2 ms: the waiting commit lands rather than ending `Busy` at the stall timeout.
#[test]
fn a_room_wait_takes_its_flush_back_from_an_undriven_worker() {
    let vfs = SimVfs::new(4);
    let mut o = sim_options(&vfs, 1);
    o.memtable_budget = 8 * CHUNK as u64;
    o.write_stall_timeout_nanos = 1_000_000_000;
    let AppOwned {
        engine,
        mut shards,
        workers,
        ..
    } = Engine::open_application_owned_with(Path::new("/db/room.phdb"), o).expect("open");
    let shard = &mut shards[0];
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // Nobody ever drives the worker. Commit until one waits for room.
    let mut waiting = None;
    for i in 0..4096u32 {
        let mut pc: PendingCommit = engine
            .submit(put(&t, &i.to_be_bytes(), 1024), Some(Durability::Buffered))
            .unwrap();
        step(&*vfs, shard, None, 32);
        match ready(&mut pc) {
            Some(r) => {
                r.expect("ok");
            }
            None => {
                waiting = Some(pc);
                break;
            }
        }
    }
    let mut pc = waiting.expect("a commit waits for room");
    // The shard takes the flush back by itself once it has waited 2 ms.
    for _ in 0..64 {
        vfs.advance(500_000);
        step(&*vfs, shard, None, 64);
        if ready(&mut pc).is_some() {
            break;
        }
    }
    assert!(
        engine.metrics().background_take_backs.1 >= 1,
        "the room wait took a flush back"
    );
    // Work that only waited for the undriven worker runs on the shard as it is: nothing
    // is dropped and redone over and over.
    assert_eq!(engine.metrics().compactions_reclaimed, 0);
    drop(workers);
    engine.close().unwrap();
    step(&*vfs, shard, None, 256);
    assert!(shard.closed().is_some());
}

// ---- real files: a compaction stopped mid-job ----

fn real_vfs() -> VfsRef {
    match std::env::var("PIGEONHOLE_IO").as_deref() {
        #[cfg(target_os = "linux")]
        Ok("uring") => pigeonhole_io::uring::UringVfs::new_application_owned().expect("io_uring"),
        _ => pigeonhole_io::pread::PreadVfs::new(2),
    }
}

/// A temporary directory of its own per test, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!("phdb-workers-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// After a close, every extent comes back: nothing unreferenced is allocated (a dropped
/// compaction's output was freed) or retired (a committed compaction's inputs were
/// reclaimed by the close, whatever reclaims were skipped before it).
fn assert_extents_back(engine: &Engine, context: &str) {
    let (allocated, retired) = engine.unreferenced_split();
    assert_eq!(
        (allocated, retired),
        (0, 0),
        "{context}: unreferenced (allocated, retired) bytes; metrics {:?}; levels {:?}",
        engine.metrics(),
        engine.sst_levels()
    );
    assert_eq!(engine.unreferenced_bytes(), 0, "{context}");
}

fn real_options(vfs: &VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(Arc::clone(vfs));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.tablet_changes = false;
    o.memtable_budget = 8 << 20;
    o.memtable_freeze_bytes = 1 << 20;
    o.background_workers = 1;
    o
}

/// Opens on real files and drives one thread, which runs both the shard and the worker
/// (under io_uring the shard's turns give it a ring of its own), until the worker is in
/// the middle of a compaction with output written. Returns the L0 SSTs it compacts.
fn stop_mid_compaction(
    vfs: &VfsRef,
    path: &Path,
) -> (Arc<Engine>, EngineShard, BackgroundWorker, Vec<u64>) {
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(path, real_options(vfs)).expect("open");
    let mut shard = shards.remove(0);
    let mut worker = workers.remove(0);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // Four overlapping L0 runs of ~1 MiB each (L0 compacts at four): a rewrite, not a move.
    engine.hold_compactions(true);
    for run in 0..4u32 {
        let mut pcs = Vec::new();
        for i in 0..4096u32 {
            let row = i * 4 + run;
            pcs.push(
                engine
                    .submit(put(&t, &row.to_be_bytes(), 256), Some(Durability::Buffered))
                    .unwrap(),
            );
            if pcs.len() == 256 {
                step(&**vfs, &mut shard, Some(&mut worker), 64);
                pcs.clear();
            }
        }
        let mut f = engine.flush_pending().unwrap();
        while ready(&mut f).is_none() {
            step(&**vfs, &mut shard, Some(&mut worker), 64);
        }
    }
    while !engine.compaction_held() {
        step(&**vfs, &mut shard, Some(&mut worker), 8);
    }
    let l0: Vec<u64> = engine
        .sst_levels()
        .into_iter()
        .filter(|(_, level, _)| *level == 0)
        .map(|(_, _, id)| id)
        .collect();
    assert!(l0.len() >= 4, "four L0 runs: {l0:?}");
    // Let the job run, one worker slice at a time, until it has output written but has
    // not finished.
    let base = engine.unreferenced_bytes();
    engine.compaction_tiny_slices(true);
    engine.hold_compactions(false);
    while engine.unreferenced_bytes() <= base {
        let now = vfs.monotonic_nanos();
        worker.run_once(now);
        assert_eq!(
            engine.compaction_commits_submitted(),
            0,
            "the job finished writing before it was stopped"
        );
    }
    engine.compaction_tiny_slices(false);
    (engine, shard, worker, l0)
}

#[cfg(target_os = "linux")]
fn ring_stats(vfs: &VfsRef) -> Option<pigeonhole_io::RingStats> {
    vfs.ring_stats()
}

#[cfg(not(target_os = "linux"))]
fn ring_stats(_vfs: &VfsRef) -> Option<pigeonhole_io::RingStats> {
    None
}

/// The reviewed case (perf287 on D284): the thread that drives the shard drives the worker
/// too, so under io_uring it has a ring; the worker stops mid-compaction, and the close,
/// from another thread, completes with only the shard driven. The worker's I/O ran inline
/// (never on the thread's ring), and the dropped compaction's output comes back.
#[test]
fn close_from_another_thread_with_a_worker_stopped_mid_compaction() {
    let dir = TempDir::new("close");
    let path = dir.0.join("w.phdb");
    let vfs = real_vfs();
    let (engine, mut shard, worker, _) = stop_mid_compaction(&vfs, &path);
    if let Some(stats) = ring_stats(&vfs) {
        assert!(stats.inline > 0, "the worker's I/O ran inline: {stats:?}");
        assert!(stats.rings <= 2, "the worker attached no ring: {stats:?}");
    }
    let closer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || engine.close())
    };
    while shard.closed().is_none() {
        let now = vfs.monotonic_nanos();
        if !shard.run_once(now + 1_000_000) {
            shard.wait(std::time::Duration::from_millis(1));
        }
    }
    assert!(shard.closed().unwrap().is_ok(), "a clean close");
    closer.join().unwrap().expect("close");
    assert!(worker.closed());
    // The dropped job's report may still be queued: the shard handles what is left.
    while shard.run_once(u64::MAX) {}
    assert_eq!(engine.metrics().compactions_reclaimed, 1);
    // The dropped job's output is free (a reopen's own preallocation would hide it).
    assert_extents_back(&engine, "after the close");
    drop((engine, shard, worker));
    // And the file reopens.
    let (engine, mut shards) = Engine::open_application_owned(&path, {
        let mut o = real_options(&vfs);
        o.background_workers = 0;
        o
    })
    .expect("reopen");
    let mut shard = shards.remove(0);
    engine.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

/// A worker dropped mid-compaction: the compaction is dropped on the shard, its output
/// freed, and its inputs stay free to compact: with no worker left the shard compacts the
/// same L0 runs itself.
#[test]
fn a_dropped_worker_mid_compaction_leaves_its_inputs_to_compact_again() {
    let dir = TempDir::new("drop");
    let path = dir.0.join("w.phdb");
    let vfs = real_vfs();
    let (engine, mut shard, worker, l0) = stop_mid_compaction(&vfs, &path);
    // Another thread keeps taking the pager's allocator, half the time, from here until the
    // compaction is redone: reclaims a dropped view only tries mostly skip (as when a pread
    // completion or a file growth holds it, CI run 40394372772), so the close's own reclaim
    // has to free the committed compaction's inputs.
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                {
                    let _held = engine.hold_pager_alloc();
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        });
        drop(worker);
        let mut turns = 0;
        while engine.metrics().compactions == 0 {
            shard.run_once(vfs.monotonic_nanos() + 1_000_000);
            turns += 1;
            assert!(turns < 1_000_000, "the inputs never compacted again");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    assert_eq!(engine.metrics().compactions_reclaimed, 1);
    assert_eq!(engine.metrics().compaction_failures, 0);
    let levels = engine.sst_levels();
    assert!(
        l0.iter().all(|id| !levels.iter().any(|(_, _, s)| s == id)),
        "the same L0 runs were compacted: {l0:?} vs {levels:?}"
    );
    engine.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
    assert_extents_back(&engine, "after the close");
}

/// perf287's must-fix on #983: a driven worker busy with two long compactions (one per
/// shard, so the take policy alone would not take anything else) still takes a flush's
/// manifest commit (the pump) and a segment rollover's WAL spare at once: they are on the
/// commit path (`Task::commit_path`), taken and run ahead of compactions. Commits and a
/// flush finish within a few worker passes while both compactions are still running.
#[test]
fn commit_path_tasks_do_not_wait_behind_compactions_on_a_driven_worker() {
    let vfs = SimVfs::new(5);
    let mut o = sim_options(&vfs, 1);
    o.shards = 2;
    o.memtable_budget = 64 * CHUNK as u64;
    o.memtable_freeze_bytes = 16 * CHUNK as u64;
    // Small segments: commits roll the WAL over, each rollover using a prepared spare.
    o.wal.segment_size = 64 << 10;
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(Path::new("/db/commit-path.phdb"), o).expect("open");
    let mut worker = workers.remove(0);
    // A table on each shard (a new table's one tablet goes to one shard).
    let mut tables: [Option<std::sync::Arc<pigeonhole_engine::TableInfo>>; 2] = [None, None];
    for i in 0..8 {
        let t = engine
            .create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let s = engine.shard_of(t.id, b"x").unwrap();
        tables[s].get_or_insert(t);
        if tables.iter().all(Option::is_some) {
            break;
        }
    }
    let tables = tables.map(|t| t.expect("a table on each shard"));
    let rows: Vec<Vec<u8>> = (0..20_000u32).map(|i| i.to_be_bytes().to_vec()).collect();
    let turn_all = |shards: &mut Vec<EngineShard>, worker: Option<&mut BackgroundWorker>| {
        let now = vfs.monotonic_nanos();
        for s in shards.iter_mut() {
            s.run_once(now + 1_000_000);
        }
        if let Some(w) = worker {
            w.run_once(now);
        }
    };
    let commit_wait =
        |shards: &mut Vec<EngineShard>, worker: &mut BackgroundWorker, b: WriteBatch| {
            let mut pc: PendingCommit = engine.submit(b, Some(Durability::Buffered)).unwrap();
            for _ in 0..256 {
                if ready(&mut pc).is_some() {
                    break;
                }
                turn_all(shards, Some(worker));
            }
            ready(&mut pc).expect("committed").expect("ok");
        };
    // Four overlapping L0 runs on each shard; the compactions they start wait in the hook.
    engine.hold_compactions(true);
    for run in 0..4usize {
        for t in &tables {
            let f = t.families[0].id;
            let mine: Vec<&Vec<u8>> = rows.iter().skip(run).step_by(4).take(4096).collect();
            for chunk in mine.chunks(64) {
                let mut b = WriteBatch::new();
                for r in chunk {
                    b.put(t.id, f, r, b"q", None, ValueRef::Bytes(&[7u8; 64]))
                        .unwrap();
                }
                commit_wait(&mut shards, &mut worker, b);
            }
        }
        let mut f = engine.flush_pending().unwrap();
        for _ in 0..1024 {
            if ready(&mut f).is_some() {
                break;
            }
            turn_all(&mut shards, Some(&mut worker));
        }
        ready(&mut f).expect("flushed").expect("ok");
    }
    for _ in 0..64 {
        turn_all(&mut shards, Some(&mut worker));
    }
    assert!(engine.compaction_held(), "the worker holds the compactions");
    // Both compactions run now, a few units per slice: hundreds of slices each.
    engine.compaction_tiny_slices(true);
    engine.hold_compactions(false);
    for _ in 0..4 {
        turn_all(&mut shards, Some(&mut worker));
    }
    let flushes = engine.metrics().flushes;
    // New commits on both shards (rolling the WAL over), and a flush that needs the pump.
    let mut passes = 0;
    for t in &tables {
        for r in rows.iter().skip(16 * 1024).take(96) {
            let mut pc: PendingCommit = engine
                .submit(put(t, r, 1024), Some(Durability::Buffered))
                .unwrap();
            while ready(&mut pc).is_none() {
                turn_all(&mut shards, Some(&mut worker));
                passes += 1;
                assert!(passes < 4096, "commits wait behind the compactions");
            }
        }
    }
    let mut f = engine.flush_pending().unwrap();
    let mut flush_passes = 0;
    while ready(&mut f).is_none() {
        turn_all(&mut shards, Some(&mut worker));
        flush_passes += 1;
        assert!(
            flush_passes < 32,
            "the flush's commit waits behind the compactions"
        );
    }
    assert!(engine.metrics().flushes > flushes);
    assert_eq!(
        engine.compaction_commits_submitted(),
        0,
        "the compactions were still running: the test is not vacuous"
    );
    engine.compaction_tiny_slices(false);
    engine.close().unwrap();
    for _ in 0..4096 {
        turn_all(&mut shards, Some(&mut worker));
        if shards.iter().all(|s| s.closed().is_some()) {
            break;
        }
    }
    assert!(shards.iter().all(|s| s.closed().is_some()));
}

/// perf287's should-fixes on #983, in one deterministic run (sim):
/// - **The extent rule** (deviation 2): with deferred I/O, after every slice a worker runs
///   of a compaction, no write or sync it submitted is still in flight
///   (`SimVfs::inline_writes_in_flight`), so freeing a taken-back compaction's output at
///   once never races a late write. A future submitted write on the output path fails here.
/// - **A stopped worker** (not dropped, no close): once it has made no pass for 100 ms the
///   shard takes its compaction back, drops it, and compacts the same inputs itself.
#[test]
fn a_stopped_worker_mid_compaction_leaves_no_write_in_flight_and_its_compaction_comes_back() {
    let vfs = SimVfs::new(6);
    let AppOwned {
        engine,
        mut shards,
        mut workers,
        ..
    } = Engine::open_application_owned_with(Path::new("/db/stopped.phdb"), {
        // One L0 run per explicit flush: no freezes of their own while the compaction is
        // held (L0 pacing would stall the commits on the simulator's frozen clock).
        let mut o = sim_options(&vfs, 1);
        o.memtable_freeze_bytes = 8 << 20;
        o
    })
    .expect("open");
    let mut shard = shards.remove(0);
    let mut worker = workers.remove(0);
    let t = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    engine.hold_compactions(true);
    for run in 0..4u32 {
        let f = t.families[0].id;
        for chunk in (0..1024u32).collect::<Vec<_>>().chunks(64) {
            let mut b = WriteBatch::new();
            for i in chunk {
                b.put(
                    t.id,
                    f,
                    &(i * 4 + run).to_be_bytes(),
                    b"q",
                    None,
                    ValueRef::Bytes(&[1; 64]),
                )
                .unwrap();
            }
            let mut pc: PendingCommit = engine.submit(b, Some(Durability::Buffered)).unwrap();
            let mut n = 0;
            while ready(&mut pc).is_none() {
                step(&*vfs, &mut shard, Some(&mut worker), 8);
                n += 1;
                assert!(
                    n < 1024,
                    "a commit never landed (run {run}, rows from {}): {:?}",
                    chunk[0],
                    engine.metrics()
                );
            }
        }
        let mut fl = engine.flush_pending().unwrap();
        let mut n = 0;
        while ready(&mut fl).is_none() {
            step(&*vfs, &mut shard, Some(&mut worker), 8);
            n += 1;
            assert!(n < 1024, "a flush never finished");
        }
    }
    step(&*vfs, &mut shard, Some(&mut worker), 64);
    assert!(engine.compaction_held());
    let l0: Vec<u64> = engine
        .sst_levels()
        .into_iter()
        .filter(|(_, level, _)| *level == 0)
        .map(|(_, _, id)| id)
        .collect();
    // The job runs a few units per slice under deferred I/O.
    vfs.set_deferred_io(true);
    engine.compaction_tiny_slices(true);
    engine.hold_compactions(false);
    let base = engine.unreferenced_bytes();
    let mut slices = 0;
    while engine.unreferenced_bytes() <= base || slices < 8 {
        worker.run_once(vfs.monotonic_nanos());
        assert_eq!(
            vfs.inline_writes_in_flight(),
            0,
            "a worker's slice left a write in flight"
        );
        vfs.complete_all_io();
        shard.run_once(vfs.monotonic_nanos() + 1_000_000);
        vfs.complete_all_io();
        slices += 1;
        assert!(slices < 4096);
    }
    assert_eq!(engine.compaction_commits_submitted(), 0, "stopped mid-job");
    // The application stops driving the worker. Nothing closes; the shard runs on.
    engine.compaction_tiny_slices(false);
    let mut turns = 0;
    while engine.metrics().compactions == 0 {
        vfs.advance(1_000_000);
        shard.run_once(vfs.monotonic_nanos() + 1_000_000);
        vfs.complete_all_io();
        turns += 1;
        assert!(turns < 1000, "the stopped worker kept the compaction");
    }
    vfs.set_deferred_io(false);
    let m = engine.metrics();
    assert_eq!(
        m.compactions_reclaimed, 1,
        "the held compaction was dropped"
    );
    assert!(m.background_take_backs.0 >= 1, "counted as a fallback");
    let levels = engine.sst_levels();
    assert!(
        l0.iter().all(|id| !levels.iter().any(|(_, _, s)| s == id)),
        "the same inputs were compacted again"
    );
    drop(worker);
    engine.close().unwrap();
    step(&*vfs, &mut shard, None, 256);
    assert!(shard.closed().expect("closed").is_ok());
    assert_eq!(engine.unreferenced_bytes(), 0);
}
