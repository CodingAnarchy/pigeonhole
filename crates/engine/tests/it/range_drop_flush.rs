//! D331: a flush that froze a memtable of tablets a range drop then retires. The drop
//! retires them (`Edit::DropTablet`, as a split or merge does) while the flush is still
//! writing; at the flush's commit their edits are skipped, as a dropped table's are, and the
//! flush's other slots commit. Before the fix the manifest took them for a split's retired
//! tablets: a debug assertion, and in release a refused flush, retried for ever (found by
//! D331 PR 5's cluster tests, a source's drop at CLEANING of a whole moved table).

use crate::gate;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pigeonhole_engine::{Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch};
use pigeonhole_format::Durability;

const DB: &str = "/db/range-drop-flush.phdb";

fn options(vfs: pigeonhole_io::VfsRef) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = 2;
    o.pin_threads = false;
    o
}

fn put(t: &TableInfo, row: &str, v: &[u8]) -> WriteBatch {
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        row.as_bytes(),
        b"q",
        None,
        ValueRef::Bytes(v),
    )
    .unwrap();
    wb
}

fn read(db: &Engine, t: &TableInfo, row: &str) -> Option<Vec<u8>> {
    db.get_latest(t.id, t.families[0].id, row.as_bytes(), b"q")
        .unwrap()
        .map(|c| match c.value() {
            ValueRef::Bytes(b) => b.to_vec(),
            other => panic!("{other:?}"),
        })
}

/// A flush holding `moved`'s memtables is held before its manifest commit; the whole of
/// `moved` is range-dropped meanwhile. Released, the flush commits (its `kept` table's rows
/// with it), the engine stays healthy, `moved` reads empty and takes writes again, and a
/// reopen reads the same. The body runs on its own thread: a failure there unwinds into the
/// engine's drop, whose close waits on the shard thread the bug panicked, so it would hang.
#[test]
fn a_flush_holding_a_range_dropped_tablet_commits_without_it() {
    gate::within(60, a_flush_holding_a_range_dropped_tablet)
        .expect("the test failed (see the panic above) or hung");
}

fn a_flush_holding_a_range_dropped_tablet() {
    let (vfs, _gate) = gate::vfs(2331);
    let db = Engine::open(Path::new(DB), options(Arc::clone(&vfs))).unwrap();
    let make = |name: &str| {
        db.create_table(name, &[("f".into(), FamilyOptions::default())])
            .unwrap()
    };
    let (moved, kept) = (make("moved"), make("kept"));
    for i in 0..64 {
        let row = format!("r{i:02}");
        db.commit(put(&moved, &row, b"old"), Some(Durability::Buffered))
            .unwrap();
        db.commit(put(&kept, &row, b"kept"), Some(Durability::Buffered))
            .unwrap();
    }
    db.hold_flush_commits(true);
    let flusher = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || db.flush())
    };
    let until = Instant::now() + Duration::from_secs(10);
    while db.held_flush_commits() == 0 {
        assert!(Instant::now() < until, "no flush reached its commit");
        std::thread::sleep(Duration::from_millis(5));
    }
    // The flush holds the memtables of `moved`'s tablets; the drop retires those tablets.
    db.drop_range(moved.id, b"", None).unwrap();
    db.hold_flush_commits(false);
    gate::within(10, move || flusher.join())
        .expect("the flush hung")
        .unwrap()
        .unwrap();
    assert!(db.failure().is_none(), "{:?}", db.failure());
    assert_eq!(read(&db, &moved, "r07"), None, "the drop's rows came back");
    assert_eq!(read(&db, &kept, "r07").as_deref(), Some(&b"kept"[..]));
    db.commit(put(&moved, "r07", b"new"), Some(Durability::Buffered))
        .unwrap();
    db.flush().unwrap();
    db.close().unwrap();
    drop(db);

    let db = Engine::open(Path::new(DB), options(vfs)).unwrap();
    let t = |name: &str| db.table(name).unwrap();
    let (moved, kept) = (t("moved"), t("kept"));
    assert_eq!(read(&db, &moved, "r07").as_deref(), Some(&b"new"[..]));
    assert_eq!(read(&db, &moved, "r08"), None);
    assert_eq!(read(&db, &kept, "r08").as_deref(), Some(&b"kept"[..]));
    db.close().unwrap();
}
