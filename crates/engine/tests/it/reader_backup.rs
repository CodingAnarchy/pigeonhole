//! A backup taken from a reader process (ICR 0049): the copy is the reader's snapshot, the
//! writer keeps committing meanwhile, the reader's pin holds the writer's memtables for the
//! whole copy (the documented cost; the writer's own backup releases them, #262), and a
//! writer restart mid-copy fails it with `SnapshotExpired`, leaving no partial file.

use crate::common;

use std::path::Path;
use std::sync::{Arc, Mutex};

use pigeonhole_engine::{
    CheckOptions, Engine, EngineOptions, Error, FamilyOptions, HookSite, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{ProcessId, Vfs, VfsRef};

const DB: &str = "/db/data.phdb";
const COPY: &str = "/db/copy.phdb";

const WRITER: ProcessId = ProcessId {
    pid: 1,
    start_time: 1,
};
const READER: ProcessId = ProcessId {
    pid: 2,
    start_time: 1,
};

fn options(vfs: &Arc<SimVfs>) -> EngineOptions {
    let mut o = common::options(Arc::clone(vfs), 1, 2 << 20);
    o.memtable_freeze_bytes = 1 << 20;
    o.tablet_changes = false;
    o
}

fn write_rows(db: &Engine, t: &TableInfo, range: std::ops::Range<u32>) {
    for i in range {
        let mut wb = WriteBatch::new();
        let row = format!("row{i:05}");
        let value = vec![(i % 251) as u8; 300];
        wb.put(
            t.id,
            t.families[0].id,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .unwrap();
        db.commit(wb, Some(Durability::Buffered)).unwrap();
    }
}

fn rows(db: &Engine) -> Vec<String> {
    let t = db.table("t").unwrap();
    let snap = db.snapshot().unwrap();
    (0..10_000u32)
        .map(|i| format!("row{i:05}"))
        .filter(|r| {
            db.get(&snap, t.id, t.families[0].id, r.as_bytes(), b"q")
                .unwrap()
                .is_some()
        })
        .collect()
}

/// A writer with 1500 rows (some flushed, the rest in the active memtable) and a reader
/// process attached to it.
fn setup(seed: u64) -> (Arc<SimVfs>, Arc<Engine>, Arc<TableInfo>, Arc<Engine>) {
    let vfs = SimVfs::new(seed);
    vfs.enter_process(WRITER);
    let db = Engine::open(Path::new(DB), options(&vfs)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    write_rows(&db, &t, 0..1000);
    db.flush().unwrap();
    write_rows(&db, &t, 1000..1500);
    vfs.enter_process(READER);
    let reader = Engine::open_reader(Path::new(DB), options(&vfs)).unwrap();
    (vfs, db, t, reader)
}

#[test]
fn a_reader_backup_is_the_readers_snapshot_while_the_writer_commits() {
    let (vfs, db, t, reader) = setup(49);
    {
        let (vfs, db, t, r) = (
            Arc::clone(&vfs),
            Arc::clone(&db),
            Arc::clone(&t),
            Arc::clone(&reader),
        );
        reader.clone().on_next(
            HookSite::BackupReleasedMemtables,
            Box::new(move || {
                // recov620's ordering. The backup's pin, as the writer sees it now.
                vfs.enter_process(WRITER);
                let (Some((_, backup_view)), _) = db.reader_pin_and_view() else {
                    panic!("the backup pins nothing");
                };
                // 1. The writer commits, flushes and compacts: the SSTs the backup reads retire
                //    at the compaction's manifest version.
                write_rows(&db, &t, 5000..5100);
                db.flush().unwrap();
                db.compact(None).unwrap();
                // 2. The reader process reads at a new snapshot. With the backup's pin held its
                //    slot stays at the backup's view; without it (no live snapshot left), the
                //    slot would move to the post-compaction view, which no longer names the
                //    retired SSTs.
                vfs.enter_process(READER);
                drop(r.snapshot().unwrap());
                // 3. A flush the size of the first, whose manifest commit reclaims what no pin
                //    covers: its SST would land in the freed extents by first fit.
                vfs.enter_process(WRITER);
                write_rows(&db, &t, 5100..6100);
                db.flush().unwrap();
                // 4. The invariant: the oldest reader pin is still at or below the backup's
                //    view, so the retired SSTs are kept.
                let (pin, _) = db.reader_pin_and_view();
                assert!(
                    pin.is_some_and(|(_, view)| view <= backup_view),
                    "the reader's pin moved past the backup's view {backup_view}: {pin:?}"
                );
                assert!(
                    db.unreferenced_bytes() > 0,
                    "the writer reclaimed SSTs the reader's backup is reading"
                );
                vfs.enter_process(READER);
            }),
        );
    }
    reader.backup(Path::new(COPY)).unwrap();
    drop(reader);

    vfs.enter_process(WRITER);
    let v: VfsRef = Arc::clone(&vfs) as _;
    let report = Engine::check(&v, Path::new(COPY), &CheckOptions::default()).unwrap();
    assert!(report.findings.is_empty(), "{:#?}", report.findings);
    let copy = Engine::open(Path::new(COPY), options(&vfs)).unwrap();
    let got = rows(&copy);
    assert_eq!(got.len(), 1500, "every row before the backup, none after");
    assert!(got.iter().all(|r| r.as_str() < "row05000"));
    copy.close().unwrap();
    assert_eq!(rows(&db).len(), 2600);
    db.close().unwrap();
}

#[test]
fn a_reader_backup_holds_the_writers_memtables_for_the_whole_copy() {
    // The writer's own backup gives its memtables back after copying them (#262,
    // milestone_b's backup_releases_the_memtable_arena_before_its_long_merge). A reader's
    // pin covers the whole view, so its backup keeps them until the copy is done: the cost
    // ICR 0049 documents.
    let (vfs, db, t, reader) = setup(50);
    let (pinned_free, _, _) = db.arena_free(0);
    let after = Arc::new(Mutex::new(None));
    {
        let (vfs, db, t, after) = (
            Arc::clone(&vfs),
            Arc::clone(&db),
            Arc::clone(&t),
            Arc::clone(&after),
        );
        reader.clone().on_next(
            HookSite::BackupReleasedMemtables,
            Box::new(move || {
                vfs.enter_process(WRITER);
                db.flush().unwrap();
                // Small commits refresh the arena counters (#277).
                for i in 0..50 {
                    write_rows(&db, &t, 6000 + i..6001 + i);
                    std::thread::yield_now();
                }
                *after.lock().unwrap() = Some(db.arena_free(0).0);
                vfs.enter_process(READER);
            }),
        );
    }
    reader.backup(Path::new(COPY)).unwrap();
    let free = after.lock().unwrap().expect("the hook ran");
    assert!(
        free < pinned_free + (400 << 10),
        "{free} bytes free mid-copy, {pinned_free} before: the reader's backup released the \
         writer's memtables while its pin still covered them"
    );
    drop(reader);
    vfs.enter_process(WRITER);
    db.close().unwrap();
}

/// The setup's 1500 rows are below the 4096 entries per slot the in-merge check counts, so the
/// restart is caught by the checks before each slot and before the copy's manifest commit.
#[test]
fn a_writer_restart_mid_copy_expires_the_reader_backup() {
    let (vfs, db, _t, reader) = setup(51);
    let next = Arc::new(Mutex::new(None));
    {
        let (vfs, next) = (Arc::clone(&vfs), Arc::clone(&next));
        let db = Mutex::new(Some(db));
        reader.clone().on_next(
            HookSite::BackupReleasedMemtables,
            Box::new(move || {
                vfs.enter_process(WRITER);
                let old = db.lock().unwrap().take().unwrap();
                old.close().unwrap();
                drop(old);
                *next.lock().unwrap() = Some(Engine::open(Path::new(DB), options(&vfs)).unwrap());
                vfs.enter_process(READER);
            }),
        );
    }
    let e = reader.backup(Path::new(COPY)).unwrap_err();
    assert!(matches!(e, Error::SnapshotExpired), "{e:?}");
    assert!(
        !vfs.exists(Path::new(COPY)).unwrap(),
        "the partial copy was removed"
    );
    drop(reader);
    vfs.enter_process(WRITER);
    let db = next
        .lock()
        .unwrap()
        .take()
        .expect("the hook reopened the writer");
    db.close().unwrap();
}
