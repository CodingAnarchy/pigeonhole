//! D333 (#2013): with `punch_freed` on (the engine tests' options), space a compaction
//! frees is given back to the filesystem, and the data reads back unchanged.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

#[test]
fn space_freed_by_compaction_is_punched_and_reads_are_unchanged() {
    let vfs = SimVfs::new(2013);
    let db = Engine::open(
        Path::new("/db/data.phdb"),
        common::options(Arc::clone(&vfs), 1, 1 << 20),
    )
    .unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let (tid, fid) = (t.id, t.families[0].id);
    let value = |round: u8| vec![round; 4 << 10];
    // Three rounds over the same rows: each compaction frees the last round's SSTs.
    for round in 1..=3u8 {
        for i in 0..1024u32 {
            let mut wb = WriteBatch::new();
            let v = value(round);
            wb.put(tid, fid, &i.to_be_bytes(), b"q", None, ValueRef::Bytes(&v))
                .unwrap();
            db.commit(wb, Some(Durability::Buffered)).unwrap();
        }
        db.flush().unwrap();
        db.compact(None).unwrap();
    }
    // A last commit's reclaim, after the compactions' views are gone, starts a pass.
    let mut wb = WriteBatch::new();
    wb.put(tid, fid, b"last", b"q", None, ValueRef::Bytes(b"x"))
        .unwrap();
    db.commit(wb, Some(Durability::Buffered)).unwrap();
    db.flush().unwrap();
    let m = db.metrics();
    assert!(m.file_punch_passes.0 > 0, "no pass: {m:?}");
    assert!(m.file_punches.0 > 0 && m.file_punch_bytes > 0, "{m:?}");
    assert_eq!(m.file_punch_passes.1, 0, "no refusals: {m:?}");
    assert!(m.file_punched_bytes > 0, "{m:?}");
    for i in (0..1024u32).step_by(97) {
        let cell = db.get_latest(tid, fid, &i.to_be_bytes(), b"q").unwrap();
        let cell = cell.unwrap_or_else(|| panic!("row {i} missing"));
        match cell.value() {
            ValueRef::Bytes(v) => assert_eq!(v, &value(3)[..], "row {i}"),
            v => panic!("row {i}: {v:?}"),
        }
    }
    db.close().unwrap();
}
