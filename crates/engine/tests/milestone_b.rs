//! Milestone B behavior: flushes to SSTs and reads over them, WAL checkpoints and one file at
//! rest after a clean close (#37), close with flush and manifest work in flight (#16), the
//! pager contracts on shrink and reclaim (#23), stream removal after a shard-count change
//! (D20), `Durability::None` carried by the next stronger commit (#50), online backup, and
//! compaction scheduling with the shared resolver.

mod common;

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;

use common::{Store, families, poll_commit};
use pigeonhole_engine::{
    Engine, EngineOptions, Error, FamilyOptions, PickerOptions, ReadSpec, ScanSpec, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{Vfs, VfsRef};
use pigeonhole_sim::{ModelOp, Sim};

const DB: &str = "/db/data.phdb";

/// Engine-owned options: tiny memtables and levels so flushes and compactions run early.
fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 1 << 20);
    o.pin_threads = false;
    o.memtable_freeze_bytes = 16 << 10;
    let mut c = PickerOptions::default();
    c.l0_trigger = 2;
    c.level_base_bytes = 48 << 10;
    c.level_multiplier = 2;
    c.max_levels = 4;
    c.target_sst_bytes = 64 << 10;
    o.compaction = c;
    o
}

fn put(wb: &mut WriteBatch, t: &pigeonhole_engine::TableInfo, row: &[u8], q: &[u8], v: &[u8]) {
    let f = t.families[0].id;
    wb.put(t.id, f, row, q, None, ValueRef::Bytes(v)).unwrap();
}

fn get_bytes(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    row: &[u8],
    q: &[u8],
) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    db.get(&snap, t.id, t.families[0].id, row, q)
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

fn row_count(db: &Engine, t: &pigeonhole_engine::TableInfo) -> usize {
    let snap = db.snapshot().unwrap();
    let mut cursor = db
        .scan(
            &snap,
            t.id,
            ScanSpec::new(Bound::Unbounded, Bound::Unbounded),
        )
        .unwrap();
    let mut n = 0;
    while cursor.next_row().unwrap() {
        n += 1;
    }
    n
}

/// The files of `/db` besides the main one.
fn sidecars(vfs: &SimVfs) -> Vec<String> {
    let mut out: Vec<String> = vfs
        .list_dir(Path::new("/db"))
        .unwrap()
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .filter(|n| n != "data.phdb" && n != "copy.phdb")
        .collect();
    out.sort();
    out
}

/// Runs every shard once; whether any has work left.
fn run_all(store: &mut Store) {
    store.run_until_idle();
}

fn write_rows(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    range: std::ops::Range<u32>,
    d: Durability,
) {
    for i in range {
        let mut wb = WriteBatch::new();
        put(
            &mut wb,
            t,
            format!("row{i:05}").as_bytes(),
            b"q",
            &vec![(i % 251) as u8; 300],
        );
        db.commit(wb, Some(d)).unwrap();
    }
}

// ---- #37: reads over SSTs, checkpoints, one file at rest ----

#[test]
fn flushed_data_reads_back_and_a_clean_close_leaves_one_file() {
    let vfs = SimVfs::new(21);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&db, &t, 0..400, Durability::Buffered);
    db.flush().unwrap();
    let m = db.metrics();
    assert!(m.flushes >= 1, "{m:?}");
    // Point gets, row reads and scans see the flushed data (and the memtable's).
    assert_eq!(
        get_bytes(&db, &t, b"row00007", b"q").unwrap(),
        vec![7u8; 300]
    );
    let snap = db.snapshot().unwrap();
    let row = db
        .read_row(&snap, t.id, b"row00399", &ReadSpec::default())
        .unwrap()
        .unwrap();
    assert_eq!(row.cells.len(), 1);
    assert_eq!(row_count(&db, &t), 400);
    // Overwrite after the flush: the newest version comes from the memtable.
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, b"row00007", b"q", b"newer");
    db.commit(wb, None).unwrap();
    assert_eq!(get_bytes(&db, &t, b"row00007", b"q").unwrap(), b"newer");
    db.close().unwrap();
    assert_eq!(sidecars(&vfs), Vec::<String>::new(), "one file at rest");

    // The reopen replays nothing (the streams are gone) and reads the same data.
    let vfs_ref: VfsRef = Arc::clone(&vfs) as VfsRef;
    let info = Engine::inspect_manifest(&vfs_ref, Path::new(DB)).unwrap();
    assert!(info.clean);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(get_bytes(&db, &t, b"row00007", b"q").unwrap(), b"newer");
    assert_eq!(row_count(&db, &t), 400);
    db.close().unwrap();
    assert_eq!(sidecars(&vfs), Vec::<String>::new());
}

#[test]
fn compaction_runs_and_preserves_reads() {
    let vfs = SimVfs::new(22);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for round in 0..6u32 {
        write_rows(
            &db,
            &t,
            round * 100..round * 100 + 100,
            Durability::Buffered,
        );
        db.flush().unwrap();
    }
    // Deletes and overwrites, then a full compaction.
    for i in 0..50u32 {
        let mut wb = WriteBatch::new();
        wb.delete_column(
            t.id,
            t.families[0].id,
            format!("row{i:05}").as_bytes(),
            b"q",
            None,
        )
        .unwrap();
        db.commit(wb, None).unwrap();
    }
    db.compact(Some(t.id)).unwrap();
    let m = db.metrics();
    assert!(m.compactions >= 1, "{m:?}");
    assert_eq!(row_count(&db, &t), 550);
    assert!(get_bytes(&db, &t, b"row00010", b"q").is_none());
    assert_eq!(
        get_bytes(&db, &t, b"row00599", b"q").unwrap(),
        vec![(599 % 251) as u8; 300]
    );
    db.close().unwrap();
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(row_count(&db, &t), 550);
    db.close().unwrap();
}

// ---- #50: Durability::None rides the next stronger commit ----

#[test]
fn durability_none_survives_power_loss_only_behind_a_stronger_commit() {
    // None then GroupSync on the same stream: the GroupSync's sync carries the None record.
    let vfs = SimVfs::new(23);
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_freeze_bytes = 4 << 20; // no flush gets in the way
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, b"a", b"q", b"none");
    db.commit(wb, Some(Durability::None)).unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, b"b", b"q", b"group-sync");
    db.commit(wb, Some(Durability::GroupSync)).unwrap();
    vfs.crash(CrashKind::Power);
    drop(db);
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(
        get_bytes(&db, &t, b"a", b"q").as_deref(),
        Some(&b"none"[..])
    );
    assert_eq!(
        get_bytes(&db, &t, b"b", b"q").as_deref(),
        Some(&b"group-sync"[..])
    );
    // None alone survives nothing: not even a process crash writes its buffered record.
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, b"c", b"q", b"alone");
    db.commit(wb, Some(Durability::None)).unwrap();
    assert_eq!(
        get_bytes(&db, &t, b"c", b"q").as_deref(),
        Some(&b"alone"[..])
    );
    vfs.crash(CrashKind::Process);
    drop(db);
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(get_bytes(&db, &t, b"c", b"q"), None);
    assert_eq!(
        get_bytes(&db, &t, b"a", b"q").as_deref(),
        Some(&b"none"[..])
    );
    db.close().unwrap();
}

// ---- D20: streams beyond a reduced shard count ----

#[test]
fn a_reduced_shard_count_flushes_and_removes_the_extra_streams() {
    let vfs = SimVfs::new(24);
    let mut o = owned(Arc::clone(&vfs), 4);
    o.memtable_freeze_bytes = 4 << 20;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let mut tables = Vec::new();
    for i in 0..4 {
        tables.push(
            db.create_table(&format!("t{i}"), &[("f".into(), FamilyOptions::default())])
                .unwrap(),
        );
    }
    for t in &tables {
        write_rows(&db, t, 0..20, Durability::Buffered);
    }
    // A process crash keeps the four streams; the reopen with two shards replays them all,
    // flushes everything recovered, checkpoints and removes streams 2 and 3.
    vfs.crash(CrashKind::Process);
    drop(db);
    assert_eq!(sidecars(&vfs).len(), 4);
    let mut o = owned(Arc::clone(&vfs), 2);
    o.memtable_freeze_bytes = 4 << 20;
    let db = Engine::open(Path::new(DB), o.clone()).unwrap();
    assert_eq!(
        sidecars(&vfs),
        ["data.phdb-wal-0", "data.phdb-wal-1"]
            .map(String::from)
            .to_vec()
    );
    for t in &tables {
        let t = db.table(&t.name).unwrap();
        assert_eq!(row_count(&db, &t), 20);
    }
    // The recovered data is in SSTs: another crash and reopen needs no stream beyond 0..2.
    write_rows(&db, &db.table("t0").unwrap(), 20..30, Durability::GroupSync);
    vfs.crash(CrashKind::Power);
    drop(db);
    let db = Engine::open(Path::new(DB), o).unwrap();
    assert_eq!(row_count(&db, &db.table("t0").unwrap()), 30);
    assert_eq!(row_count(&db, &db.table("t3").unwrap()), 20);
    db.close().unwrap();
    assert_eq!(sidecars(&vfs), Vec::<String>::new());
}

// ---- #16: close with flush and manifest work in flight ----

#[test]
fn close_with_flushes_in_flight_loses_nothing_and_needs_no_replay() {
    let sim = Sim::new(25);
    let vfs = sim.vfs();
    let mut cfg = common::Config::quiet(0);
    cfg.shards = 2;
    let mut store = Store::open_cfg(&vfs, 2, &cfg).unwrap();
    // Enough writes that several flushes queue up; close immediately after the last commit.
    let mut last = 0;
    for i in 0..300u32 {
        let row = format!("row{i:06}");
        let op = ModelOp::Put {
            table: common::table_of(row.as_bytes()).into(),
            row: row.into_bytes(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: None,
            value: vec![1u8; 200],
        };
        let batch = store.batch(&[op]).unwrap();
        let mut pc = store
            .engine
            .submit(batch, Some(Durability::Buffered))
            .unwrap();
        last = loop {
            match poll_commit(&mut pc) {
                Poll::Ready(r) => break r.unwrap().seqno,
                Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
            }
        };
    }
    store.engine.close().unwrap();
    run_all(&mut store);
    drop(store);
    assert_eq!(
        sidecars(&vfs),
        Vec::<String>::new(),
        "checkpointed and removed"
    );
    let vfs_ref: VfsRef = Arc::clone(&vfs) as VfsRef;
    let info = Engine::inspect_manifest(&vfs_ref, Path::new(DB)).unwrap();
    assert!(info.clean);
    let store = Store::open_cfg(&vfs, 2, &cfg).unwrap();
    let snap = store.engine.snapshot().unwrap();
    assert!(snap.seqno() >= last);
    let mut rows = 0;
    for t in common::TABLES {
        rows += store
            .scan(&snap, t, Bound::Unbounded, Bound::Unbounded, 1)
            .unwrap()
            .len();
    }
    assert_eq!(rows, 300);
}

#[test]
fn a_crash_during_close_recovers_every_acknowledged_commit() {
    // Sweep crash points through a close: whatever the point, the reopen has every
    // acknowledged commit.
    let mut n = 1;
    let mut crashed_closes = 0;
    loop {
        let vfs = SimVfs::new(26);
        let mut o = owned(Arc::clone(&vfs), 2);
        o.memtable_freeze_bytes = 8 << 10;
        let db = Engine::open(Path::new(DB), o.clone()).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        write_rows(&db, &t, 0..60, Durability::GroupSync);
        let mut plan = FaultPlan::none();
        plan.crash_after_ops = Some(vfs.mutating_ops() + n);
        vfs.set_faults(plan);
        let r = db.close();
        let alive = r.is_ok() && vfs.exists(Path::new(DB)).is_ok();
        vfs.set_faults(FaultPlan::none());
        drop(db);
        let db = Engine::open(Path::new(DB), o).unwrap();
        let t = db.table("t").unwrap();
        assert_eq!(row_count(&db, &t), 60, "crash point {n}");
        db.close().unwrap();
        if alive {
            break;
        }
        crashed_closes += 1;
        n += 1;
        assert!(n < 5_000, "runaway sweep");
    }
    assert!(crashed_closes > 0);
}

/// Waits (bounded) for `cond`.
fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed().as_secs() < 30,
            "timed out waiting for {what}"
        );
        std::thread::yield_now();
    }
}

#[test]
fn the_final_close_waits_for_a_background_manifest_commit() {
    // #78: the last shard closed while a compaction's root commit was durable but not yet
    // finished (`end` pending on its pump). The close's own commits ran without the
    // manifest writer's exclusion, were prepared from the same writer state, and the clean
    // root left out the compaction (in debug builds the compaction's `end` then retired the
    // old manifest extents a second time).
    let vfs = SimVfs::new(29);
    let vfs_ref: VfsRef = vfs.clone();
    let mut o = owned(Arc::clone(&vfs), 2);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for round in 0..2u32 {
        write_rows(
            &db,
            &t,
            round * 100..round * 100 + 100,
            Durability::GroupSync,
        );
        db.flush().unwrap();
    }
    db.park_manifest_commits(true);
    let compact = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.compact(None))
    };
    wait_for("the compaction's commit to park", || {
        db.manifest_commit_parked()
    });
    let close = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.close())
    };
    // Every shard closes; the final close must wait for the parked commit.
    wait_for("the final close", || {
        db.final_close_pending() || close.is_finished()
    });
    assert!(
        !close.is_finished(),
        "the close committed over an unfinished commit"
    );
    db.park_manifest_commits(false);
    // The close answers the compaction's waiter (`Closed`); its commit still completes.
    let _ = compact.join().unwrap();
    close.join().unwrap().unwrap();
    let compacted = db.take_compactions();
    assert_eq!(compacted.len(), 1);
    drop(db);

    // The clean root is the close's own commit, made after the compaction's.
    let info = Engine::inspect_manifest(&vfs_ref, Path::new(DB)).unwrap();
    assert!(info.clean);
    assert!(
        info.version > compacted[0].manifest_version,
        "{} <= {}",
        info.version,
        compacted[0].manifest_version
    );
    assert!(info.checkpoints.values().all(|l| *l == Default::default()));
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(row_count(&db, &t), 200);
    db.close().unwrap();
}

// ---- #23: shrink never relocates in-flight output; reclaim after commits ----

#[test]
fn shrink_releases_space_after_compaction_and_skips_unpublished_output() {
    let vfs = SimVfs::new(27);
    // Only explicit compactions: a background one allocating between the two size
    // measurements would make the file grow under the shrink.
    let mut o = owned(Arc::clone(&vfs), 1);
    o.compaction.l0_trigger = u32::MAX;
    o.compaction.level_base_bytes = u64::MAX;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for round in 0..8u32 {
        write_rows(
            &db,
            &t,
            round * 100..round * 100 + 100,
            Durability::Buffered,
        );
        db.flush().unwrap();
    }
    db.compact(None).unwrap();
    let before = vfs
        .open(Path::new(DB), pigeonhole_io::OpenOptions::read())
        .unwrap()
        .len()
        .unwrap();
    let released = db.shrink().unwrap();
    let after = vfs
        .open(Path::new(DB), pigeonhole_io::OpenOptions::read())
        .unwrap()
        .len()
        .unwrap();
    assert!(after <= before, "{after} > {before}");
    assert_eq!(before - after, released);
    assert_eq!(row_count(&db, &t), 800);
    // Shrink while a flush is in flight: the unpublished output is never moved, and nothing
    // is lost.
    write_rows(&db, &t, 800..900, Durability::Buffered);
    let db2 = Arc::clone(&db);
    let flush = std::thread::spawn(move || db2.flush());
    let _ = db.shrink().unwrap();
    flush.join().unwrap().unwrap();
    assert_eq!(row_count(&db, &t), 900);
    db.close().unwrap();
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    assert_eq!(row_count(&db, &db.table("t").unwrap()), 900);
    db.close().unwrap();
}

// ---- backup ----

#[test]
fn backup_is_a_consistent_single_file_copy() {
    let vfs = SimVfs::new(28);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("f".into(), FamilyOptions::default()),
                ("g".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    write_rows(&db, &t, 0..300, Durability::Buffered);
    db.flush().unwrap();
    write_rows(&db, &t, 300..350, Durability::Buffered);
    let g = t.families[1].id;
    let mut wb = WriteBatch::new();
    wb.put(t.id, g, b"row00001", b"x", None, ValueRef::I64(7))
        .unwrap();
    db.commit(wb, None).unwrap();
    // Writers keep going while the backup runs.
    let db2 = Arc::clone(&db);
    let t2 = Arc::clone(&t);
    let writer =
        std::thread::spawn(move || write_rows(&db2, &t2, 1000..1100, Durability::Buffered));
    db.backup(Path::new("/db/copy.phdb")).unwrap();
    writer.join().unwrap();
    assert!(
        matches!(db.backup(Path::new("/db/copy.phdb")), Err(Error::Io(_))),
        "exists"
    );
    db.close().unwrap();

    let copy = Engine::open(Path::new("/db/copy.phdb"), owned(Arc::clone(&vfs), 3)).unwrap();
    let ct = copy.table("t").unwrap();
    let n = row_count(&copy, &ct);
    assert!(
        (350..=450).contains(&n),
        "{n} rows: the snapshot's, not more"
    );
    assert_eq!(
        get_bytes(&copy, &ct, b"row00123", b"q").unwrap(),
        vec![123u8; 300]
    );
    let snap = copy.snapshot().unwrap();
    let cell = copy
        .get(&snap, ct.id, ct.families[1].id, b"row00001", b"x")
        .unwrap()
        .unwrap();
    assert_eq!(cell.value(), ValueRef::I64(7));
    // The copy is a database of its own: writes and a clean close work.
    write_rows(&copy, &ct, 2000..2010, Durability::GroupSync);
    copy.close().unwrap();
    assert_eq!(sidecars(&vfs), Vec::<String>::new());
}

// ---- stalls ----

#[test]
fn deep_l0_stalls_writes_without_refusing_them() {
    let vfs = SimVfs::new(29);
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_freeze_bytes = 8 << 10;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // Writes outrun compaction (2 KiB cells against 8 KiB memtables and 64 KiB SSTs):
    // once the tree is a few levels deep, a compaction takes longer than the token bucket
    // lets commits through, and the writer is held, never refused.
    let mut written = 0u32;
    while written < 20_000 && db.metrics().stalls.0 == 0 {
        for _ in 0..50 {
            let mut wb = WriteBatch::new();
            put(
                &mut wb,
                &t,
                format!("row{written:05}").as_bytes(),
                b"q",
                &vec![1u8; 2048],
            );
            db.commit(wb, Some(Durability::None)).unwrap();
            written += 1;
        }
    }
    let m = db.metrics();
    assert!(m.flushes >= 4 && m.compactions >= 1, "{m:?}");
    assert!(m.stalls.0 > 0, "writers were never stalled: {m:?}");
    assert_eq!(row_count(&db, &t), written as usize);
    db.close().unwrap();
}

/// A sim-driven variant: the same single-file-at-rest property under the scheduler with
/// four tables on three shards and cross-shard commits.
#[test]
fn sim_run_closes_to_one_file_and_reopens_without_replay() {
    let sim = Sim::new(30);
    let vfs = sim.vfs();
    let mut cfg = common::Config::quiet(0);
    cfg.shards = 3;
    let mut store = Store::open_cfg(&vfs, 3, &cfg).unwrap();
    for i in 0..120u32 {
        let a = format!("row{i:06}");
        let b = format!("row{:06}", i + 1);
        let ops: Vec<ModelOp> = [a, b]
            .into_iter()
            .map(|row| ModelOp::Put {
                table: common::table_of(row.as_bytes()).into(),
                row: row.into_bytes(),
                family: "g".into(),
                qualifier: b"q".to_vec(),
                ts: None,
                value: vec![2u8; 150],
            })
            .collect();
        let batch = store.batch(&ops).unwrap();
        let mut pc = store
            .engine
            .submit(batch, Some(Durability::Buffered))
            .unwrap();
        loop {
            match poll_commit(&mut pc) {
                Poll::Ready(r) => {
                    r.unwrap();
                    break;
                }
                Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
            }
        }
    }
    store.engine.close().unwrap();
    run_all(&mut store);
    drop(store);
    assert_eq!(sidecars(&vfs), Vec::<String>::new());
    let store = Store::open_cfg(&vfs, 3, &cfg).unwrap();
    let snap = store.engine.snapshot().unwrap();
    let mut rows = 0;
    for t in common::TABLES {
        rows += store
            .scan(&snap, t, Bound::Unbounded, Bound::Unbounded, 1)
            .unwrap()
            .len();
    }
    assert_eq!(rows, 121);
    let _ = families();
}

// ---- D29: values above the inline threshold are pinned, never copied ----

#[test]
fn large_memtable_values_are_pinned_not_copied() {
    let vfs = SimVfs::new(29);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    for (i, len) in [16usize, 100, 1000, 8192].into_iter().enumerate() {
        let mut wb = WriteBatch::new();
        put(
            &mut wb,
            &t,
            format!("row{i}").as_bytes(),
            b"q",
            &vec![i as u8 + 1; len],
        );
        db.commit(wb, None).unwrap();
    }
    let snap = db.snapshot().unwrap();
    for (i, len) in [16usize, 100, 1000, 8192].into_iter().enumerate() {
        let row = format!("row{i}");
        for c in [
            db.get_latest(t.id, f, row.as_bytes(), b"q").unwrap(),
            db.get(&snap, t.id, f, row.as_bytes(), b"q").unwrap(),
        ] {
            let c = c.expect("present");
            assert_eq!(c.stored().len(), len + 1);
            let shape = format!("{c:?}");
            if len <= 128 {
                assert!(shape.contains("Inline("), "{row}: {shape}");
            } else {
                // Memtable-resident: a pinned arena slice, whatever the resolver copied.
                assert!(shape.contains("Arena("), "{row}: {shape}");
            }
        }
    }
    db.close().unwrap();
}

// ---- a clean close forgets the removed streams' checkpoints ----

#[test]
fn writes_after_a_clean_reopen_survive_a_power_loss() {
    let vfs = SimVfs::new(31);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&db, &t, 0..50, Durability::Sync);
    db.flush().unwrap();
    db.close().unwrap();
    assert_eq!(sidecars(&vfs), Vec::<String>::new());
    // Fresh WAL streams, whose LSNs start over: the manifest must not keep the old ones'
    // checkpoints, or recovery would skip these records.
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    write_rows(&db, &t, 50..100, Durability::Sync);
    vfs.crash(CrashKind::Power);
    drop(db);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    for i in [0u32, 49, 50, 77, 99] {
        assert_eq!(
            get_bytes(&db, &t, format!("row{i:05}").as_bytes(), b"q").unwrap(),
            vec![(i % 251) as u8; 300],
            "row {i}"
        );
    }
    db.close().unwrap();
}

// ---- the manifest queue never strands a request pushed while the writer lets go ----

#[test]
fn a_manifest_request_pushed_in_the_release_window_is_committed() {
    let vfs = SimVfs::new(41);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&db, &t, 0..10, Durability::Buffered);
    let before = db.snapshot().unwrap().view().manifest_version();
    // Without the release-then-re-check rule the second request would wait for an
    // unrelated commit; with it, both are committed before the probe returns.
    assert!(db.probe_manifest_release_window().unwrap());
    assert!(db.snapshot().unwrap().view().manifest_version() >= before + 2);
    db.close().unwrap();
}

// ---- a full arena stalls writers while a slow flush frees it, never refuses them ----

/// A VFS whose syncs take real time, as a slow disk's would.
#[derive(Debug)]
struct SlowSyncVfs {
    inner: Arc<SimVfs>,
    delay: std::time::Duration,
}

#[derive(Debug)]
struct SlowSyncFile {
    inner: pigeonhole_io::FileRef,
    delay: std::time::Duration,
}

impl Vfs for SlowSyncVfs {
    fn open(
        &self,
        path: &Path,
        opts: pigeonhole_io::OpenOptions,
    ) -> pigeonhole_io::Result<pigeonhole_io::FileRef> {
        let inner = self.inner.open(path, opts)?;
        Ok(Arc::new(SlowSyncFile {
            inner,
            delay: self.delay,
        }))
    }
    fn remove(&self, path: &Path) -> pigeonhole_io::Result<()> {
        self.inner.remove(path)
    }
    fn exists(&self, path: &Path) -> pigeonhole_io::Result<bool> {
        self.inner.exists(path)
    }
    fn list_dir(&self, dir: &Path) -> pigeonhole_io::Result<Vec<std::path::PathBuf>> {
        self.inner.list_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> pigeonhole_io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.sync_dir(dir)
    }
    fn open_shared(
        &self,
        name: &str,
        dir: Option<&Path>,
        len: u64,
        mode: pigeonhole_io::SharedOpen,
    ) -> pigeonhole_io::Result<pigeonhole_io::SharedRegion> {
        self.inner.open_shared(name, dir, len, mode)
    }
    fn remove_shared(&self, name: &str, dir: Option<&Path>) -> pigeonhole_io::Result<()> {
        self.inner.remove_shared(name, dir)
    }
    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
    fn monotonic_nanos(&self) -> u64 {
        self.inner.monotonic_nanos()
    }
    fn current_process(&self) -> pigeonhole_io::ProcessId {
        self.inner.current_process()
    }
    fn process_alive(&self, process: pigeonhole_io::ProcessId) -> bool {
        self.inner.process_alive(process)
    }
}

impl pigeonhole_io::File for SlowSyncFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> pigeonhole_io::Result<()> {
        self.inner.write_at(buf, offset)
    }
    fn submit_read(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        self.inner.submit_read(buf, offset)
    }
    fn submit_write(&self, buf: pigeonhole_io::IoBuf, offset: u64) -> pigeonhole_io::Completion {
        self.inner.submit_write(buf, offset)
    }
    fn sync_data(&self) -> pigeonhole_io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.sync_data()
    }
    fn submit_sync_data(&self) -> pigeonhole_io::Completion<()> {
        std::thread::sleep(self.delay);
        self.inner.submit_sync_data()
    }
    fn sync_all(&self) -> pigeonhole_io::Result<()> {
        std::thread::sleep(self.delay);
        self.inner.sync_all()
    }
    fn len(&self) -> pigeonhole_io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.set_len(len)
    }
    fn allocate(&self, offset: u64, len: u64) -> pigeonhole_io::Result<()> {
        self.inner.allocate(offset, len)
    }
    fn lock(&self, byte: u64, mode: pigeonhole_io::LockMode) -> pigeonhole_io::Result<()> {
        self.inner.lock(byte, mode)
    }
    fn unlock(&self, byte: u64) -> pigeonhole_io::Result<()> {
        self.inner.unlock(byte)
    }
    fn identity(&self) -> pigeonhole_io::Result<pigeonhole_io::FileIdentity> {
        self.inner.identity()
    }
    fn is_local(&self) -> pigeonhole_io::Result<bool> {
        self.inner.is_local()
    }
}

#[test]
fn a_full_arena_stalls_writers_until_a_slow_flush_frees_it() {
    // Syncs take 2 ms, so flushes lag the writer far behind: the arena fills many times
    // over, and every commit waits for room rather than being refused.
    let vfs: VfsRef = Arc::new(SlowSyncVfs {
        inner: SimVfs::new(43),
        delay: std::time::Duration::from_millis(2),
    });
    let mut o = EngineOptions::new(Arc::clone(&vfs));
    o.create_if_missing = true;
    o.shards = 1;
    o.pin_threads = false;
    o.memtable_budget = 256 << 10;
    o.memtable_freeze_bytes = 16 << 10;
    o.wal.segment_size = 256 << 10;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    // Four writers, 32 KiB batches: the arena (256 KiB) fills in a few commits while each
    // flush spends milliseconds in syncs.
    let value = vec![9u8; 1024];
    std::thread::scope(|s| {
        for w in 0..4u32 {
            let (db, t, value) = (&db, &t, &value);
            s.spawn(move || {
                for i in 0..60u32 {
                    let mut wb = WriteBatch::new();
                    for j in 0..32u32 {
                        put(
                            &mut wb,
                            t,
                            format!("w{w}-{i:03}-{j:02}").as_bytes(),
                            b"q",
                            value,
                        );
                    }
                    db.commit(wb, Some(Durability::Buffered)).unwrap();
                }
            });
        }
    });
    let m = db.metrics();
    assert!(m.flushes >= 8, "{m:?}");
    assert!(m.stalls.0 > 0, "writers were never stalled: {m:?}");
    assert_eq!(row_count(&db, &t), 4 * 60 * 32);
    db.close().unwrap();
}

// ---- #66: the purge record counts entries an earlier compaction dropped as inputs ----

/// Commits `ops` on an application-owned store, driving the shards; returns the seqno.
fn commit_ops(store: &mut Store, vfs: &Arc<SimVfs>, ops: &[ModelOp]) -> u64 {
    let batch = store.batch(ops).unwrap();
    let mut pc = store.engine.submit(batch, Some(Durability::Sync)).unwrap();
    loop {
        match poll_commit(&mut pc) {
            Poll::Ready(r) => return r.unwrap().seqno,
            Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
        }
    }
}

#[test]
fn a_purge_record_covers_entries_dropped_by_an_earlier_compaction() {
    // A put written after a row delete with an older timestamp is hidden at every read
    // point, so a non-bottommost compaction drops it; the bottommost compaction that later
    // purges the row delete must still report it as an input (`max_seqno`), or the model's
    // purge (D74) brings it back while the engine has rightly dropped it.
    let sim = Sim::new(66);
    let vfs = sim.vfs();
    let cfg = common::Config::quiet(0);
    let mut store = Store::open_cfg(&vfs, 1, &cfg).unwrap();
    let row = b"row000001".to_vec();
    let table = common::table_of(&row).to_owned();
    let put = |q: &[u8], ts: Option<u64>| ModelOp::Put {
        table: table.clone(),
        row: row.clone(),
        family: "g".into(),
        qualifier: q.to_vec(),
        ts,
        value: vec![7; 16],
    };
    let maintain = |store: &mut Store, compact: bool| {
        let m = if compact {
            store.engine.compact_pending(None)
        } else {
            store.engine.flush_pending()
        };
        store.drive(&vfs, m.unwrap(), || true).unwrap();
        store.run_until_idle();
    };
    // The last level holds data, so the L0 compaction below is not bottommost.
    commit_ops(&mut store, &vfs, &[put(b"q0", None)]);
    maintain(&mut store, true);
    let deleted = commit_ops(
        &mut store,
        &vfs,
        &[ModelOp::DeleteRow {
            table: table.clone(),
            row: row.clone(),
        }],
    );
    maintain(&mut store, false);
    let hidden = commit_ops(&mut store, &vfs, &[put(b"q2", Some(1))]);
    // A second L0 file: the picker compacts L0 into a middle level, dropping the put.
    maintain(&mut store, false);
    let (_, family) = store.ids(&table, "g");
    let records: Vec<_> = store
        .engine
        .take_compactions()
        .into_iter()
        .filter(|r| r.family == family)
        .collect();
    assert!(
        records
            .iter()
            .any(|r| !r.bottommost && r.max_seqno >= hidden),
        "no middle-level compaction took the put: {records:?}"
    );
    maintain(&mut store, true);
    let full: Vec<_> = store
        .engine
        .take_compactions()
        .into_iter()
        .filter(|r| r.family == family)
        .collect();
    let last = full.last().expect("the full compaction's record");
    assert!(last.bottommost, "{last:?}");
    assert!(
        last.max_seqno >= hidden,
        "max_seqno {} leaves out the dropped put at seqno {hidden} (row delete at {deleted})",
        last.max_seqno
    );
    let snap = store.engine.snapshot().unwrap();
    assert_eq!(store.get(&snap, &row, "g", b"q2").unwrap(), None);
}
