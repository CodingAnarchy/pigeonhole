//! Behavior the decisions and issues pin down: open order and last-one-out (#20, D37),
//! application-owned `compaction_cores` (#22, D40), interrupted creates (#24, D59), family
//! order (#26, D39), the clean flag (D57), a poisoned pager (#23, D58), conditional writes,
//! optimistic transactions, metrics, and the real file backend in engine-owned mode.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{Store, families, poll_commit};
use pigeonhole_engine::{
    COUNTER_TS, Engine, EngineOptions, Error, FamilyKind, FamilyOptions, Predicate, ReadSpec,
    ScanSpec, ValuePredicate, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_format::shm::directory_name;
use pigeonhole_io::sim::{CrashKind, FaultPlan, SimVfs};
use pigeonhole_io::{OpenOptions, ProcessId, SharedOpen, Vfs, VfsRef};
use pigeonhole_shm::WriterLock;
use pigeonhole_sim::Sim;

const DB: &str = "/db/data.phdb";

/// Engine-owned options over `vfs` (threads, so blocking calls work in a test).
fn owned(vfs: Arc<SimVfs>, shards: usize) -> EngineOptions {
    let mut o = common::options(vfs, shards, 4 << 20);
    o.pin_threads = false;
    o
}

fn put(
    wb: &mut WriteBatch,
    t: &pigeonhole_engine::TableInfo,
    fam: &str,
    row: &[u8],
    q: &[u8],
    v: &[u8],
) {
    let f = t.family(fam).unwrap().id;
    wb.put(t.id, f, row, q, None, ValueRef::Bytes(v)).unwrap();
}

fn get_bytes(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    fam: &str,
    row: &[u8],
    q: &[u8],
) -> Option<Vec<u8>> {
    let snap = db.snapshot().unwrap();
    let f = t.family(fam).unwrap().id;
    db.get(&snap, t.id, f, row, q)
        .unwrap()
        .map(|c| common::value_bytes(c.value()))
}

// ---- #22 / D40 ----

#[test]
fn application_owned_refuses_compaction_cores_before_opening_anything() {
    let vfs = SimVfs::new(1);
    let mut o = common::options(Arc::clone(&vfs), 1, 1 << 20);
    o.compaction_threads = 1;
    let err = Engine::open_application_owned(Path::new(DB), o.clone())
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::InvalidArgument(ref m) if m.contains("compaction_cores")),
        "{err}"
    );
    assert!(!vfs.exists(Path::new(DB)).unwrap(), "no file is created");
    // Nothing holds the writer byte: a writer opens at once.
    let mut o2 = o.clone();
    o2.compaction_threads = 0;
    let (db, shards) = Engine::open_application_owned(Path::new(DB), o2).unwrap();
    db.close().unwrap();
    drop(shards);
    // Engine-owned mode accepts the same options (a compaction thread is started).
    let db = Engine::open(Path::new(DB), owned_with(o, 1)).unwrap();
    db.close().unwrap();
}

fn owned_with(mut o: EngineOptions, threads: usize) -> EngineOptions {
    o.compaction_threads = threads;
    o.pin_threads = false;
    o
}

// ---- #24 / D59 ----

#[test]
fn interrupted_create_is_refused_and_never_deleted() {
    // Under the simulator a power loss during `Pager::create` reverts the unsynced
    // directory entry, so the file either exists complete or not at all: sweep that.
    let mut n = 1;
    let mut absent = 0;
    let mut complete = 0;
    loop {
        let vfs = SimVfs::new(7);
        let mut plan = FaultPlan::none();
        plan.crash_after_ops = Some(n);
        vfs.set_faults(plan);
        let crashed = match Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)) {
            Ok(db) => {
                let alive = db.snapshot().is_ok();
                let _ = db.close();
                !alive
            }
            Err(_) => true,
        };
        if !crashed {
            break;
        }
        vfs.set_faults(FaultPlan::none());
        if vfs.exists(Path::new(DB)).unwrap() {
            complete += 1;
        } else {
            absent += 1;
        }
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        db.close().unwrap();
        n += 1;
        assert!(n < 10_000, "runaway sweep");
    }
    eprintln!("create sweep: {absent} absent, {complete} complete, {n} points");
    assert!(absent > 0 && complete > 0);

    // A filesystem that makes the entry durable before the data leaves a short file with
    // no valid superblock (decision D59): refused with a message, never removed or changed.
    for (len, interrupted) in [(4096u64, true), (64 * 1024, true), (64 * 1024 + 1, false)] {
        let vfs = SimVfs::new(8);
        let file = vfs
            .open(Path::new(DB), OpenOptions::read_write_create())
            .unwrap();
        file.write_at(&vec![0u8; len as usize], 0).unwrap();
        file.sync_all().unwrap();
        vfs.sync_dir(Path::new("/db")).unwrap();
        drop(file);
        for create_if_missing in [true, false] {
            let mut o = owned(Arc::clone(&vfs), 1);
            o.create_if_missing = create_if_missing;
            let err = Engine::open(Path::new(DB), o).unwrap_err();
            match err {
                Error::Corruption(msg) => {
                    assert_eq!(
                        msg.contains("interrupted create"),
                        interrupted,
                        "{len}: {msg}"
                    );
                    assert!(msg.contains("data.phdb"), "names the file: {msg}");
                }
                e => panic!("{len}: unexpected {e}"),
            }
            let f = vfs.open(Path::new(DB), OpenOptions::read()).unwrap();
            assert_eq!(f.len().unwrap(), len, "never changed");
            let mut buf = vec![1u8; len as usize];
            f.read_at(&mut buf, 0).unwrap();
            assert!(buf.iter().all(|b| *b == 0), "never written");
        }
        // After the application deletes it, a create succeeds.
        vfs.remove(Path::new(DB)).unwrap();
        let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
        db.close().unwrap();
    }
}

// ---- #26 / D39 ----

#[test]
fn families_come_in_creation_or_requested_order() {
    let vfs = SimVfs::new(3);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let defs = ["z", "a", "m"].map(|n| (n.to_owned(), FamilyOptions::default()));
    let t = db.create_table("t", &defs).unwrap();
    let mut wb = WriteBatch::new();
    for fam in ["a", "m", "z"] {
        put(&mut wb, &t, fam, b"r", b"q", fam.as_bytes());
    }
    db.commit(wb, None).unwrap();
    let snap = db.snapshot().unwrap();
    let names = |row: &pigeonhole_engine::RowData| -> Vec<String> {
        row.cells
            .iter()
            .map(|c| {
                t.families
                    .iter()
                    .find(|f| f.id == c.family)
                    .unwrap()
                    .name
                    .clone()
            })
            .collect()
    };
    let row = db
        .read_row(&snap, t.id, b"r", &ReadSpec::default())
        .unwrap()
        .unwrap();
    assert_eq!(names(&row), ["z", "a", "m"]);
    let mut spec = ReadSpec::default();
    spec.families = vec![
        t.family("m").unwrap().id,
        t.family("a").unwrap().id,
        t.family("m").unwrap().id,
    ];
    let row = db.read_row(&snap, t.id, b"r", &spec).unwrap().unwrap();
    assert_eq!(names(&row), ["m", "a"]);
    // Scans follow the same order.
    let mut sspec = ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
    sspec.read = spec;
    let mut cursor = db.scan(&snap, t.id, sspec).unwrap();
    assert!(cursor.next_row().unwrap());
    let mut seen = Vec::new();
    while let Some(c) = cursor.next_cell().unwrap() {
        seen.push(
            t.families
                .iter()
                .find(|f| f.id == c.family)
                .unwrap()
                .name
                .clone(),
        );
    }
    assert_eq!(seen, ["m", "a"]);
    db.close().unwrap();
}

// ---- D57 ----

#[test]
fn replay_happens_even_after_a_clean_close() {
    let vfs = SimVfs::new(4);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    db.create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    db.close().unwrap(); // clean flag set
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let mut wb = WriteBatch::new();
    put(
        &mut wb,
        &db.table("t").unwrap(),
        "f",
        b"r",
        b"q",
        b"after-clean-close",
    );
    db.commit(wb, Some(Durability::Buffered)).unwrap();
    // No root commit happened since the clean close; kill the process.
    vfs.crash(CrashKind::Process);
    drop(db);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(
        get_bytes(&db, &t, "f", b"r", b"q").as_deref(),
        Some(&b"after-clean-close"[..])
    );
    db.close().unwrap();
}

// ---- #23 / D58 ----

#[test]
fn a_failed_manifest_commit_poisons_the_writer_until_reopen() {
    let vfs = SimVfs::new(5);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"q", b"durable");
    db.commit(wb, None).unwrap();
    let mut plan = FaultPlan::none();
    plan.enospc_after_bytes = Some(0);
    vfs.set_faults(plan);
    let err = db
        .create_table("u", &[("f".into(), FamilyOptions::default())])
        .unwrap_err();
    assert!(matches!(err, Error::NoSpace | Error::Io(_)), "{err}");
    vfs.set_faults(FaultPlan::none());
    // Every later write fails until reopen, even though the device has space again.
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"q", b"later");
    assert!(matches!(db.commit(wb, None), Err(Error::Io(_))));
    assert!(matches!(db.create_table("v", &[]), Err(Error::Io(_))));
    drop(db);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db.table("t").unwrap();
    assert!(
        db.table("u").is_none(),
        "the failed commit never became durable"
    );
    assert_eq!(
        get_bytes(&db, &t, "f", b"r", b"q").as_deref(),
        Some(&b"durable"[..])
    );
    db.close().unwrap();
}

// ---- #20 / D37: last one out ----

#[test]
fn a_closing_reader_never_cleans_up_under_an_opening_writer() {
    let vfs = SimVfs::new(6);
    let p_writer = ProcessId {
        pid: 1,
        start_time: 1,
    };
    let p_reader = ProcessId {
        pid: 2,
        start_time: 1,
    };
    let p_next = ProcessId {
        pid: 3,
        start_time: 1,
    };
    vfs.enter_process(p_writer);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"q", b"v1");
    db.commit(wb, None).unwrap();

    vfs.enter_process(p_reader);
    let reader = Engine::open_reader(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let rt = reader.table("t").unwrap();
    assert_eq!(
        get_bytes(&reader, &rt, "f", b"r", b"q").as_deref(),
        Some(&b"v1"[..])
    );

    // The writer closes (the reader is still attached, so nothing is removed).
    vfs.enter_process(p_writer);
    db.close().unwrap();
    drop(db);
    let identity = vfs
        .open(Path::new(DB), OpenOptions::read())
        .unwrap()
        .identity()
        .unwrap();
    let dir_name = directory_name(identity.device, identity.inode);
    let vfs_ref: VfsRef = Arc::clone(&vfs) as VfsRef;
    assert!(
        vfs_ref
            .open_shared(&dir_name, None, 4096, SharedOpen::Attach)
            .is_ok()
    );

    // The next writer is between `WriterLock::acquire` and `Presence::acquire`.
    vfs.enter_process(p_next);
    let mut rw = OpenOptions::read();
    rw.write = true;
    let next_file = vfs.open(Path::new(DB), rw).unwrap();
    let lock = WriterLock::acquire(&next_file).unwrap();

    // The reader closes now: it is the last present process, but a writer holds the byte.
    vfs.enter_process(p_reader);
    reader.close().unwrap();
    drop(reader);
    assert!(
        vfs.exists(Path::new("/db/data.phdb-wal-0")).unwrap(),
        "WAL files survive"
    );
    assert!(
        vfs_ref
            .open_shared(&dir_name, None, 4096, SharedOpen::Attach)
            .is_ok(),
        "the shared-memory directory survives"
    );

    // The writer finishes opening; later readers attach and read.
    drop(lock);
    drop(next_file);
    vfs.enter_process(p_next);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(
        get_bytes(&db, &t, "f", b"r", b"q").as_deref(),
        Some(&b"v1"[..])
    );
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"q", b"v2");
    db.commit(wb, None).unwrap();
    vfs.enter_process(p_reader);
    let reader = Engine::open_reader(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let rt = reader.table("t").unwrap();
    assert_eq!(
        get_bytes(&reader, &rt, "f", b"r", b"q").as_deref(),
        Some(&b"v2"[..])
    );
    reader.close().unwrap();
    vfs.enter_process(p_next);
    db.close().unwrap();
    // Last one out: the region is gone.
    assert!(
        vfs_ref
            .open_shared(&dir_name, None, 4096, SharedOpen::Attach)
            .is_err()
    );
}

#[test]
fn a_reader_follows_the_writer_across_commits_and_a_restart() {
    let vfs = SimVfs::new(8);
    let p_writer = ProcessId {
        pid: 1,
        start_time: 1,
    };
    let p_reader = ProcessId {
        pid: 2,
        start_time: 1,
    };
    vfs.enter_process(p_writer);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 3)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    vfs.enter_process(p_reader);
    let reader = Engine::open_reader(Path::new(DB), owned(Arc::clone(&vfs), 3)).unwrap();
    assert!(matches!(
        reader.commit(WriteBatch::new(), None),
        Err(Error::ReadOnly)
    ));
    let rt = reader.table("t").unwrap();
    for i in 0..20u32 {
        vfs.enter_process(p_writer);
        let mut wb = WriteBatch::new();
        put(
            &mut wb,
            &t,
            "f",
            format!("row{i:02}").as_bytes(),
            b"q",
            &i.to_le_bytes(),
        );
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        vfs.enter_process(p_reader);
        let snap = reader.snapshot().unwrap();
        let mut cursor = reader
            .scan(
                &snap,
                rt.id,
                ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            )
            .unwrap();
        let mut rows = 0;
        while cursor.next_row().unwrap() {
            rows += 1;
        }
        assert_eq!(rows, i + 1);
    }
    // A table added later is visible to the reader through the manifest.
    vfs.enter_process(p_writer);
    db.create_table("u", &[("g".into(), FamilyOptions::default())])
        .unwrap();
    vfs.enter_process(p_reader);
    let _ = reader.snapshot().unwrap();
    assert!(reader.table("u").is_some());
    // Writer restart: the reader re-attaches to the new generation.
    vfs.enter_process(p_writer);
    db.close().unwrap();
    drop(db);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 3)).unwrap();
    let t = db.table("t").unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"row99", b"q", b"new");
    db.commit(wb, None).unwrap();
    vfs.enter_process(p_reader);
    assert_eq!(
        get_bytes(&reader, &rt, "f", b"row99", b"q").as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(
        get_bytes(&reader, &rt, "f", b"row00", b"q").as_deref(),
        Some(&0u32.to_le_bytes()[..])
    );
    reader.close().unwrap();
    vfs.enter_process(p_writer);
    db.close().unwrap();
}

// ---- conditional writes and transactions ----

#[test]
fn check_and_mutate_is_atomic_on_the_owner() {
    let vfs = SimVfs::new(9);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let absent = Predicate::Absent {
        family: f,
        qualifier: b"lock".to_vec(),
    };
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"lock", b"me");
    let (applied, info) = db
        .check_and_mutate(t.id, b"r", &absent, wb.clone(), None)
        .unwrap();
    assert!(applied && info.is_some());
    let (applied, info) = db
        .check_and_mutate(t.id, b"r", &absent, wb.clone(), None)
        .unwrap();
    assert!(!applied && info.is_none());
    let value = Predicate::Value {
        family: f,
        qualifier: b"lock".to_vec(),
        predicate: ValuePredicate::Equals(b"me".to_vec()),
    };
    let mut wb2 = WriteBatch::new();
    wb2.delete_column(t.id, f, b"r", b"lock", None).unwrap();
    let (applied, _) = db.check_and_mutate(t.id, b"r", &value, wb2, None).unwrap();
    assert!(applied);
    assert_eq!(get_bytes(&db, &t, "f", b"r", b"lock"), None);
    // A batch touching another row is refused.
    let mut wb3 = WriteBatch::new();
    put(&mut wb3, &t, "f", b"other", b"q", b"x");
    assert!(matches!(
        db.check_and_mutate(t.id, b"r", &absent, wb3, None),
        Err(Error::InvalidArgument(_))
    ));
    // Many concurrent claimants: exactly one wins.
    let db2 = Arc::clone(&db);
    let winners: usize = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let db = Arc::clone(&db2);
                let t = Arc::clone(&t);
                s.spawn(move || {
                    let mut wb = WriteBatch::new();
                    put(&mut wb, &t, "f", b"r", b"lock", &[i as u8]);
                    let pred = Predicate::Absent {
                        family: f,
                        qualifier: b"lock".to_vec(),
                    };
                    usize::from(db.check_and_mutate(t.id, b"r", &pred, wb, None).unwrap().0)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });
    assert_eq!(winners, 1);
    db.close().unwrap();
}

#[test]
fn optimistic_transactions_abort_on_conflict() {
    let vfs = SimVfs::new(10);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"acct", b"balance", b"10");
    db.commit(wb, None).unwrap();

    let mut txn = db.begin().unwrap();
    assert_eq!(
        txn.get(t.id, f, b"acct", b"balance")
            .unwrap()
            .map(|c| common::value_bytes(c.value())),
        Some(b"10".to_vec())
    );
    // Someone else changes the row in between.
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"acct", b"balance", b"11");
    db.commit(wb, None).unwrap();
    txn.batch()
        .put(t.id, f, b"acct", b"balance", None, ValueRef::Bytes(b"20"))
        .unwrap();
    assert!(matches!(txn.commit(None), Err(Error::Conflict)));
    assert_eq!(
        get_bytes(&db, &t, "f", b"acct", b"balance").as_deref(),
        Some(&b"11"[..])
    );

    // Without interference the transaction commits.
    let mut txn = db.begin().unwrap();
    let _ = txn.get(t.id, f, b"acct", b"balance").unwrap();
    txn.batch()
        .put(t.id, f, b"acct", b"balance", None, ValueRef::Bytes(b"20"))
        .unwrap();
    txn.commit(None).unwrap();
    assert_eq!(
        get_bytes(&db, &t, "f", b"acct", b"balance").as_deref(),
        Some(&b"20"[..])
    );
    db.close().unwrap();
}

// ---- catalog, limits, lifecycle ----

#[test]
fn catalog_changes_persist_and_tables_can_be_dropped() {
    let vfs = SimVfs::new(11);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    assert!(matches!(
        db.create_table("t", &[]),
        Err(Error::TableExists(_))
    ));
    let t = db.add_family(t.id, "g", FamilyOptions::default()).unwrap();
    assert!(matches!(
        db.add_family(t.id, "g", FamilyOptions::default()),
        Err(Error::FamilyExists(_))
    ));
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "g", b"r", b"q", b"in-g");
    db.commit(wb, None).unwrap();
    let u = db
        .create_table("u", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut wb = WriteBatch::new();
    put(&mut wb, &u, "f", b"r", b"q", b"in-u");
    db.commit(wb, None).unwrap();
    db.drop_table(u.id).unwrap();
    assert!(db.table("u").is_none());
    let mut wb = WriteBatch::new();
    put(&mut wb, &u, "f", b"r", b"q", b"after-drop");
    assert!(matches!(
        db.commit(wb, None),
        Err(Error::FamilyNotFound(_) | Error::TableNotFound(_))
    ));
    db.close().unwrap();
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db.table("t").unwrap();
    assert_eq!(
        t.families
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["f", "g"]
    );
    assert_eq!(
        get_bytes(&db, &t, "g", b"r", b"q").as_deref(),
        Some(&b"in-g"[..])
    );
    assert!(db.table("u").is_none());
    assert_eq!(db.tables().len(), 1);
    db.close().unwrap();
}

#[test]
fn a_value_at_the_documented_limit_fits_the_arena() {
    // Issue #141 (8-9 F3): D16 allows a value up to half a shard's arena, but the arena
    // accounting charged every entry twice, so such a value (or a batch of a few large
    // values adding up to it) was refused as if it could never fit. One large entry wastes
    // at most a chunk, not its own size.
    for (budget, seed) in [(1u64 << 20, 31), (8 << 20, 32), (64 << 20, 33)] {
        let vfs = SimVfs::new(seed);
        let mut o = owned(Arc::clone(&vfs), 1);
        o.memtable_budget = budget;
        o.wal.segment_size = (2 * budget).max(4 << 20);
        let db = Engine::open(Path::new(DB), o).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let half = (budget / 2) as usize;
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, "f", b"half", b"q", &vec![3u8; half]);
        db.commit(wb, Some(Durability::None))
            .unwrap_or_else(|e| panic!("budget {budget}: a value of half the arena: {e}"));
        // A batch of four quarter-arena-sized values adds up to the same and fits too.
        let mut wb = WriteBatch::new();
        for i in 0..4u8 {
            put(&mut wb, &t, "f", &[b'b', i], b"q", &vec![i; half / 4]);
        }
        db.commit(wb, Some(Durability::None))
            .unwrap_or_else(|e| panic!("budget {budget}: four eighths of the arena: {e}"));
        assert_eq!(
            get_bytes(&db, &t, "f", b"half", b"q").map(|v| v.len()),
            Some(half)
        );
        db.close().unwrap();
    }
}

#[test]
fn a_value_with_free_bytes_but_no_long_enough_run_waits_instead_of_poisoning() {
    // Issue #141 review: free bytes scattered across the arena do not hold an entry larger
    // than a chunk, which needs one contiguous run. With enough free bytes in total but no
    // run long enough, the commit used to be admitted, failed to allocate at apply, and
    // poisoned the shard. It now waits for room (here in vain: a stall) and the shard goes on.
    let vfs = SimVfs::new(34);
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 4 << 20;
    o.write_stall_timeout_nanos = 300_000_000;
    // The layout this test builds islands in: 64 chunks of 16 KiB (arenas are otherwise
    // sized for their slots, #283).
    o.tablet_changes = false;
    let chunk = 16usize << 10;
    o.arena_chunk_bytes = Some(chunk);
    // `x` freezes on its own once it holds 93 chunks.
    o.memtable_freeze_bytes = (93 * chunk) as u64;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let table = |name: &str| {
        db.create_table(name, &[("f".into(), FamilyOptions::default())])
            .unwrap()
    };
    let (x, p1, p2) = (table("x"), table("p1"), table("p2"));
    let commit = |t: &pigeonhole_engine::TableInfo, row: &[u8], len: usize| {
        let mut wb = WriteBatch::new();
        put(&mut wb, t, "f", row, b"q", &vec![5u8; len]);
        db.commit(wb, Some(Durability::None))
    };
    let allocated = || {
        let (free, _, len) = db.arena_free(0);
        (len - free) as usize / chunk
    };
    // `x` takes chunks from the bottom, one per 15 KiB row; `p1` and `p2` each take one
    // chunk where `x` stopped, so they sit at chunks 31 and 63 as islands.
    let mut row = 0u32;
    let mut grow_x_to = |n: usize| {
        while allocated() < n {
            assert!(row < 200, "x never reached {n} chunks");
            commit(&x, &row.to_be_bytes(), 15 << 10).unwrap();
            row += 1;
        }
    };
    grow_x_to(31);
    commit(&p1, b"island", 1).unwrap();
    grow_x_to(63);
    commit(&p2, b"island", 1).unwrap();
    // `x` freezes at 93 chunks (its fresh memtable is the third island) and its old chunks
    // come back once flushed. Write until that flush has happened: counting chunks would
    // race it (the flush may free them before the count is read).
    let flushed = db.metrics().flushes;
    while db.metrics().flushes == flushed {
        assert!(row < 200, "x never froze and flushed");
        commit(&x, &row.to_be_bytes(), 15 << 10).unwrap();
        row += 1;
    }
    // Once `x`'s old chunks are reclaimed (the half's own reservation reclaims them if
    // nothing has yet), the free runs are 31, 31, 31 and 32 chunks: 1.5 MiB free, but half
    // the arena (D16's limit: 33 chunks with its node and the prefix) fits none of them.
    let half = 512usize << 10;
    let r = commit(&x, b"half", half);
    assert!(
        matches!(r, Ok(_) | Err(Error::Busy)),
        "a value that fits no run: {r:?}"
    );
    assert!(
        db.arena_run_waits(0) >= 1,
        "the half never met an arena with enough free bytes but no run for it"
    );
    // The shard is not poisoned.
    commit(&p2, b"after", 100).unwrap();
    commit(&x, b"after", 100).unwrap();
    db.close().unwrap();
}

#[test]
fn value_limits_arena_pressure_and_closed_handles() {
    let vfs = SimVfs::new(12);
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_budget = 1 << 20;
    o.memtable_freeze_bytes = 64 << 10;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let big = vec![7u8; 600 << 10];
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"r", b"q", &big);
    assert!(
        matches!(db.commit(wb, None), Err(Error::ValueTooLarge)),
        "over half the arena (D16)"
    );
    // Fill the arena several times over: a full arena waits for a flush, never refuses and
    // never applies a commit in part.
    let mut written = 0;
    for i in 0..400u32 {
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, "f", &i.to_be_bytes(), b"q", &vec![1u8; 4096]);
        db.commit(wb, Some(Durability::None)).unwrap();
        written += 1;
    }
    assert!(db.metrics().flushes >= 1, "flushes freed the arena");
    // A batch that could never fit, even in an empty arena, is refused at once.
    let mut wb = WriteBatch::new();
    for i in 0..300u32 {
        put(
            &mut wb,
            &t,
            "f",
            &(1000 + i).to_be_bytes(),
            b"q",
            &vec![1u8; 4096],
        );
    }
    // Not a stall (issue #141): its own non-retryable error, and no stall is counted (this
    // test's writes flush long before the arena fills; the refusal used to be counted).
    let stalls = db.metrics().stalls.0;
    assert!(matches!(
        db.commit(wb, Some(Durability::None)),
        Err(Error::BatchTooLarge)
    ));
    assert_eq!(
        db.metrics().stalls.0,
        stalls,
        "a never-fits refusal is no stall"
    );
    // Everything written is readable, from memtables and SSTs alike.
    let snap = db.snapshot().unwrap();
    let mut cursor = db
        .scan(
            &snap,
            t.id,
            ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
        .unwrap();
    let mut rows = 0;
    while cursor.next_row().unwrap() {
        rows += 1;
    }
    assert_eq!(rows, written);
    db.flush().unwrap();
    db.close().unwrap();
    assert!(matches!(
        db.commit(WriteBatch::new(), None),
        Err(Error::Closed)
    ));
    assert!(matches!(db.create_table("x", &[]), Err(Error::Closed)));
}

#[test]
fn metrics_count_commits_per_level() {
    let vfs = SimVfs::new(13);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    for (i, d) in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
        Durability::Sync,
    ]
    .into_iter()
    .enumerate()
    {
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, "f", &[i as u8], b"q", b"v");
        let info = db.commit(wb, Some(d)).unwrap();
        assert_eq!(info.durability, d);
    }
    db.set_default_durability(Durability::Buffered);
    assert_eq!(db.default_durability(), Durability::Buffered);
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f", b"x", b"q", b"v");
    assert_eq!(
        db.commit(wb, None).unwrap().durability,
        Durability::Buffered
    );
    let m = db.metrics();
    assert_eq!(m.commits, [1, 2, 1, 2]);
    assert!(
        m.commit_latency_nanos
            .iter()
            .all(|l| l[0] <= l[1] && l[1] <= l[2])
    );
    db.close().unwrap();
}

#[test]
fn get_latest_matches_a_fresh_snapshot() {
    let vfs = SimVfs::new(14);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let big = vec![9u8; 4000];
    for v in [&b"small"[..], &big[..]] {
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, "f", b"r", b"q", v);
        db.commit(wb, None).unwrap();
        let latest = db.get_latest(t.id, f, b"r", b"q").unwrap().unwrap();
        assert_eq!(latest.stored()[1..], *v);
        assert_eq!(latest.value(), ValueRef::Bytes(v));
        assert_eq!(get_bytes(&db, &t, "f", b"r", b"q").unwrap(), v);
    }
    assert!(db.get_latest(t.id, f, b"missing", b"q").unwrap().is_none());
    db.close().unwrap();
}

#[test]
fn second_writer_is_refused() {
    let vfs = SimVfs::new(15);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    vfs.enter_process(ProcessId {
        pid: 2,
        start_time: 1,
    });
    assert!(matches!(
        Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)),
        Err(Error::WriterLocked)
    ));
    vfs.enter_process(ProcessId {
        pid: 1,
        start_time: 1,
    });
    db.close().unwrap();
}

// ---- the real file backend ----

#[test]
fn engine_owned_mode_on_real_files() {
    let dir = std::env::temp_dir().join(format!("pigeonhole-engine-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("data.phdb");
    let vfs: VfsRef = pigeonhole_io::pread::PreadVfs::new(2);
    let mut o = EngineOptions::new(Arc::clone(&vfs));
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o.memtable_budget = 4 << 20;
    o.memtable_freeze_bytes = 1 << 20;
    o.reader_slots = 4;
    o.wal.segment_size = 8 * 32 * 1024;
    {
        let db = Engine::open(&path, o.clone()).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        for i in 0..50u32 {
            let mut wb = WriteBatch::new();
            put(
                &mut wb,
                &t,
                "f",
                format!("row{i:03}").as_bytes(),
                b"q",
                &i.to_le_bytes(),
            );
            db.commit(
                wb,
                Some(
                    [
                        Durability::GroupSync,
                        Durability::Buffered,
                        Durability::Sync,
                    ][i as usize % 3],
                ),
            )
            .unwrap();
        }
        db.close().unwrap();
    }
    {
        let db = Engine::open(&path, o.clone()).unwrap();
        let t = db.table("t").unwrap();
        assert_eq!(
            get_bytes(&db, &t, "f", b"row049", b"q").as_deref(),
            Some(&49u32.to_le_bytes()[..])
        );
        let snap = db.snapshot().unwrap();
        let mut cursor = db
            .scan(
                &snap,
                t.id,
                ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
            )
            .unwrap();
        let mut rows = 0;
        while cursor.next_row().unwrap() {
            rows += 1;
        }
        assert_eq!(rows, 50);
        db.close().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- application-owned inline commits ----

#[test]
fn commit_local_runs_inline_on_the_owning_shard() {
    let sim = Sim::new(16);
    let vfs = sim.vfs();
    let mut store = Store::open(&vfs, 2, 4 << 20, &families()).unwrap();
    let rows: Vec<(String, u16)> = (0..40)
        .map(|i| {
            let r = format!("row{i:06}");
            let view = store.engine.snapshot().unwrap().view().clone();
            let t = store.tables[common::table_of(r.as_bytes())].id;
            let shard = view.tablets().route(t, r.as_bytes()).unwrap().1.0;
            (r, shard)
        })
        .collect();
    let (row0, shard0) = rows.iter().find(|(_, s)| *s == 0).unwrap().clone();
    let (row1, _) = rows.iter().find(|(_, s)| *s == 1).unwrap().clone();
    let batch = store
        .batch(&[pigeonhole_sim::ModelOp::Put {
            table: common::table_of(row0.as_bytes()).into(),
            row: row0.clone().into_bytes(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: None,
            value: b"inline".to_vec(),
        }])
        .unwrap();
    // Own row: committed inline (the group runs before `commit_local` returns), durable
    // once the shard's sync completes.
    let mut pc = store.shards[shard0 as usize]
        .commit_local(batch, Some(Durability::Buffered))
        .unwrap();
    let info = loop {
        match poll_commit(&mut pc) {
            std::task::Poll::Ready(r) => break r.unwrap(),
            std::task::Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
        }
    };
    assert_eq!(info.seqno, 1);
    // Another shard's row: submitted through the queue instead.
    let batch = store
        .batch(&[pigeonhole_sim::ModelOp::Put {
            table: common::table_of(row1.as_bytes()).into(),
            row: row1.into_bytes(),
            family: "f".into(),
            qualifier: b"q".to_vec(),
            ts: None,
            value: b"queued".to_vec(),
        }])
        .unwrap();
    let mut pc = store.shards[shard0 as usize]
        .commit_local(batch, None)
        .unwrap();
    let info = loop {
        match poll_commit(&mut pc) {
            std::task::Poll::Ready(r) => break r.unwrap(),
            std::task::Poll::Pending => store.step_shards(vfs.monotonic_nanos()),
        }
    };
    assert_eq!(info.seqno, 2);
    let snap = store.engine.snapshot().unwrap();
    assert_eq!(
        store
            .get(&snap, row0.as_bytes(), "f", b"q")
            .unwrap()
            .unwrap()
            .value,
        b"inline"
    );
    store.engine.close().unwrap();
    for _ in 0..4 {
        store.step_shards(vfs.monotonic_nanos());
    }
}

// ---- #148 / D164: test-hook recording is opt-in ----

#[test]
fn test_hook_history_records_only_once_turned_on() {
    let vfs = SimVfs::new(148);
    let db = Engine::open(Path::new(DB), owned(vfs, 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let commit = |row: &[u8]| {
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, "f", row, b"q", b"v");
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    };
    let compact = || {
        db.flush().unwrap();
        commit(b"again");
        db.flush().unwrap();
        db.compact(None).unwrap();
    };
    // Off at open: nothing is kept, however long the run.
    commit(b"a");
    compact();
    assert!(db.take_appended().is_empty());
    assert!(db.take_compactions().is_empty());
    // On: both record; off again: they stop.
    db.record_history(true);
    commit(b"b");
    compact();
    assert_eq!(db.take_appended().len(), 2);
    assert!(!db.take_compactions().is_empty());
    db.record_history(false);
    commit(b"c");
    assert!(db.take_appended().is_empty());
    db.close().unwrap();
}

// ---- D179: counter families ----

/// `(timestamp, i64)` of every version of `row`/`q` in `fam`.
fn counter_versions(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    fam: &str,
    row: &[u8],
) -> Vec<(u64, i64)> {
    let snap = db.snapshot().unwrap();
    let mut spec = ReadSpec::default();
    spec.families = vec![t.family(fam).unwrap().id];
    db.read_row(&snap, t.id, row, &spec)
        .unwrap()
        .map(|r| {
            r.cells
                .iter()
                .map(|c| match c.data.value() {
                    ValueRef::I64(v) => (c.data.timestamp(), v),
                    v => panic!("not an i64: {v:?}"),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn counter_family(ttl_micros: u64) -> FamilyOptions {
    FamilyOptions {
        merge_operator: "pigeonhole.i64_add".into(),
        kind: FamilyKind::Counter,
        ttl_micros,
        ..FamilyOptions::default()
    }
}

#[test]
fn counter_family_operands_share_a_timestamp_and_deletes_hide_only_older_ones() {
    let vfs = SimVfs::new(41);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table("t", &[("c".into(), counter_family(0))])
        .unwrap();
    let c = t.family("c").unwrap().id;
    let incr = |q: &[u8], d: i64| {
        let mut wb = WriteBatch::new();
        wb.merge(t.id, c, b"r", q, ValueRef::I64(d)).unwrap();
        db.commit(wb, None).unwrap();
    };
    incr(b"n", 2);
    incr(b"n", 3);
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 5)]);
    // Buckets are versions of their own.
    let mut wb = WriteBatch::new();
    wb.merge_at(t.id, c, b"r", b"n", 100, ValueRef::I64(7))
        .unwrap();
    wb.merge_at(t.id, c, b"r", b"n", 200, ValueRef::I64(1))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert_eq!(
        counter_versions(&db, &t, "c", b"r"),
        [(200, 1), (100, 7), (COUNTER_TS, 5)]
    );
    // A column delete hides what was written before it, not a later increment at a covered
    // timestamp; flushes and compactions keep it that way.
    let mut wb = WriteBatch::new();
    wb.delete_column(t.id, c, b"r", b"n", None).unwrap();
    db.commit(wb, None).unwrap();
    assert_eq!(counter_versions(&db, &t, "c", b"r"), []);
    incr(b"n", 4);
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 4)]);
    db.flush().unwrap();
    incr(b"n", 1);
    db.compact(None).unwrap();
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 5)]);
    // put_i64 sets the counter; later increments add to it.
    let mut wb = WriteBatch::new();
    wb.put(t.id, c, b"r", b"n", None, ValueRef::I64(100))
        .unwrap();
    db.commit(wb, None).unwrap();
    incr(b"n", 1);
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 101)]);
    db.close().unwrap();
}

#[test]
fn counter_family_write_rules() {
    let vfs = SimVfs::new(42);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 1)).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("c".into(), counter_family(0)),
                ("ttl".into(), counter_family(1_000_000)),
                (
                    "legacy".into(),
                    FamilyOptions {
                        merge_operator: "pigeonhole.i64_add".into(),
                        ..FamilyOptions::default()
                    },
                ),
                ("plain".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    let id = |f: &str| t.family(f).unwrap().id;
    let refused = |wb: WriteBatch, what: &str| {
        let err = db.commit(wb, None).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidArgument(m) if m.contains(what)),
            "{err}"
        );
    };
    // A counter family holds only i64s.
    let mut wb = WriteBatch::new();
    wb.put(t.id, id("c"), b"r", b"n", None, ValueRef::Bytes(b"x"))
        .unwrap();
    refused(wb, "only i64");
    let mut wb = WriteBatch::new();
    wb.merge(t.id, id("c"), b"r", b"n", ValueRef::Bytes(b"x"))
        .unwrap();
    refused(wb, "only i64");
    // With a TTL its fixed timestamp would expire at once: buckets only.
    let mut wb = WriteBatch::new();
    wb.merge(t.id, id("ttl"), b"r", b"n", ValueRef::I64(1))
        .unwrap();
    refused(wb, "TTL");
    let now = vfs.now_micros();
    let mut wb = WriteBatch::new();
    wb.merge_at(t.id, id("ttl"), b"r", b"n", now, ValueRef::I64(1))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert_eq!(counter_versions(&db, &t, "ttl", b"r"), [(now, 1)]);
    // A bucket timestamp is for counter families only.
    for f in ["legacy", "plain"] {
        let mut wb = WriteBatch::new();
        wb.merge_at(t.id, id(f), b"r", b"n", 5, ValueRef::I64(1))
            .unwrap();
        refused(wb, "not a counter family");
    }
    // A family without an operator refuses operands.
    let mut wb = WriteBatch::new();
    wb.merge(t.id, id("plain"), b"r", b"n", ValueRef::I64(1))
        .unwrap();
    refused(wb, "no merge operator");
    // A 0.1.0-style family (the operator, standard kind) is unchanged: operands take the
    // commit timestamp and fold across timestamps (D41).
    for d in [1, 2] {
        let mut wb = WriteBatch::new();
        wb.merge(t.id, id("legacy"), b"r", b"n", ValueRef::I64(d))
            .unwrap();
        db.commit(wb, None).unwrap();
    }
    let legacy = counter_versions(&db, &t, "legacy", b"r");
    assert_eq!(legacy.len(), 1);
    assert!(legacy[0].0 > COUNTER_TS && legacy[0].1 == 3, "{legacy:?}");
    // A counter family sums with the built-in operator only.
    let mut bad = counter_family(0);
    bad.merge_operator = String::new();
    let err = db.add_family(t.id, "bad", bad).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    db.close().unwrap();
}

/// #295 (D186): writes of one counter cell in one commit apply in order: operands add up,
/// and an operand after a put adds to it. Other families keep D34 (the last write wins).
#[test]
fn counter_writes_in_one_commit_combine() {
    let vfs = SimVfs::new(295);
    let db = Engine::open(Path::new(DB), owned(Arc::clone(&vfs), 2)).unwrap();
    let t = db
        .create_table(
            "t",
            &[
                ("c".into(), counter_family(0)),
                (
                    "legacy".into(),
                    FamilyOptions {
                        merge_operator: "pigeonhole.i64_add".into(),
                        ..FamilyOptions::default()
                    },
                ),
            ],
        )
        .unwrap();
    let (c, legacy) = (t.family("c").unwrap().id, t.family("legacy").unwrap().id);
    let mut wb = WriteBatch::new();
    wb.merge(t.id, c, b"r", b"n", ValueRef::I64(1)).unwrap();
    wb.merge(t.id, c, b"r", b"n", ValueRef::I64(2)).unwrap();
    wb.put(t.id, c, b"s", b"n", None, ValueRef::I64(5)).unwrap();
    wb.merge(t.id, c, b"s", b"n", ValueRef::I64(1)).unwrap();
    wb.merge_at(t.id, c, b"s", b"n", 40, ValueRef::I64(3))
        .unwrap();
    wb.merge_at(t.id, c, b"s", b"n", 40, ValueRef::I64(4))
        .unwrap();
    wb.merge(t.id, legacy, b"r", b"n", ValueRef::I64(1))
        .unwrap();
    wb.merge(t.id, legacy, b"r", b"n", ValueRef::I64(2))
        .unwrap();
    db.commit(wb, None).unwrap();
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 3)]);
    assert_eq!(
        counter_versions(&db, &t, "c", b"s"),
        [(40, 7), (COUNTER_TS, 6)]
    );
    let legacy_sum: Vec<i64> = counter_versions(&db, &t, "legacy", b"r")
        .into_iter()
        .map(|(_, v)| v)
        .collect();
    assert_eq!(legacy_sum, [2], "D34 still collapses other families");
    // A transaction's batch combines the same way.
    let mut txn = db.begin().unwrap();
    txn.batch()
        .merge(t.id, c, b"r", b"n", ValueRef::I64(10))
        .unwrap();
    txn.batch()
        .merge(t.id, c, b"r", b"n", ValueRef::I64(20))
        .unwrap();
    txn.commit(None).unwrap();
    assert_eq!(counter_versions(&db, &t, "c", b"r"), [(COUNTER_TS, 33)]);
    db.close().unwrap();
}

/// #283 with D16: with chunks sized for many slots (here 1 KiB-sized chunks in a 1 MiB arena
/// with tablet changes off), an entry larger than a chunk still takes a contiguous run of
/// them. Values just above a chunk, several chunks long, and as large as D16 allows are
/// admitted, applied and read back, and the shard goes on writing (nothing is admitted that
/// then fails to allocate at apply, which would poison it).
#[test]
fn values_larger_than_a_small_chunk_are_admitted_and_applied() {
    let vfs = SimVfs::new(2831);
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 4 << 20;
    o.tablet_changes = false;
    o.write_stall_timeout_nanos = 300_000_000;
    let db = Engine::open(Path::new(DB), o).unwrap();
    // 64 tables of 4 families: 256 slots on the one shard, so a reopen sizes chunks for
    // them (1 MiB / (4 x 256) = 1 KiB) instead of 4 KiB (1 MiB / 256).
    let fams: Vec<(String, FamilyOptions)> = (0..4)
        .map(|i| (format!("f{i}"), FamilyOptions::default()))
        .collect();
    for i in 0..64 {
        db.create_table(&format!("t{i}"), &fams).unwrap();
    }
    db.close().unwrap();
    let mut o = owned(Arc::clone(&vfs), 1);
    o.memtable_budget = 1 << 20;
    o.wal.segment_size = 4 << 20;
    o.tablet_changes = false;
    o.write_stall_timeout_nanos = 300_000_000;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db.table("t0").unwrap();
    let chunk = 1024usize;
    let half = 512usize << 10;
    for (i, len) in [chunk + 1, 3 * chunk + 7, 64 * chunk, half - 4096]
        .into_iter()
        .enumerate()
    {
        let mut wb = WriteBatch::new();
        let row = [b'v', i as u8];
        put(&mut wb, &t, "f0", &row, b"q", &vec![i as u8; len]);
        db.commit(wb, Some(Durability::None))
            .unwrap_or_else(|e| panic!("a value of {len} bytes: {e}"));
        assert_eq!(
            get_bytes(&db, &t, "f0", &row, b"q").map(|v| v.len()),
            Some(len)
        );
        db.flush().unwrap();
    }
    // The shard is not poisoned: a small write after them commits.
    let mut wb = WriteBatch::new();
    put(&mut wb, &t, "f1", b"after", b"q", b"ok");
    db.commit(wb, None).unwrap();
    db.close().unwrap();
}
