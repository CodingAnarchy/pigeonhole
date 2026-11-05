//! A scan whose reader stops a row early moves to the next row, not to the rest of that one
//! (#1286, simp724's A1 on it: an async nearest-neighbour search stops a row at its column
//! limit). The lane that handed out the last cell read still held it; it was left out of the
//! skip, so the row's remaining cells came back as a "next" row with the same key.

use crate::common;

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ScanSpec, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

#[test]
fn a_row_left_after_one_cell_is_skipped_whole() {
    let vfs = SimVfs::new(1286);
    let db = Engine::open(
        Path::new("/db/data.phdb"),
        common::options(Arc::clone(&vfs), 1, 64 << 20),
    )
    .unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let mut wb = WriteBatch::new();
    for (row, cells) in [(&b"a"[..], 5u8), (b"b", 2), (b"c", 3)] {
        for q in 0..cells {
            wb.put(t.id, f, row, &[b'q', q], None, ValueRef::Bytes(b"v"))
                .unwrap();
        }
    }
    db.commit(wb, Some(Durability::Buffered)).unwrap();
    for flushed in [false, true] {
        let snap = db.snapshot().unwrap();
        let mut cur = db
            .scan(
                &snap,
                t.id,
                ScanSpec::new(Bound::Unbounded, Bound::Unbounded),
            )
            .unwrap();
        let mut rows = Vec::new();
        while cur.next_row().unwrap() {
            rows.push(cur.row().to_vec());
            // Read one cell of each row, then leave the rest.
            assert!(cur.next_cell().unwrap().is_some());
        }
        assert_eq!(
            rows,
            [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "each row once (flushed: {flushed})"
        );
        drop(cur);
        drop(snap);
        db.flush().unwrap();
    }
    db.close().unwrap();
}
