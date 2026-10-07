//! The file stays bounded under repeated small compactions (#106): each output takes an
//! extent sized to what it holds, not a target-size one, and retired inputs are reclaimed.

mod common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ValueRef, WriteBatch};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};

const DB: &str = "/db/data.phdb";

fn file_len(vfs: &SimVfs) -> u64 {
    vfs.open(Path::new(DB), OpenOptions::read())
        .unwrap()
        .len()
        .unwrap()
}

#[test]
fn small_compactions_keep_the_file_bounded() {
    let vfs = SimVfs::new(106);
    // The default 64 MiB target: before #106 every output reserved a whole 64 MiB extent.
    let db = Engine::open(Path::new(DB), common::options(Arc::clone(&vfs), 1, 1 << 20)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    const ROWS: u32 = 100;
    const VALUE: usize = 300;
    let mut max_len = 0;
    for round in 0..100u32 {
        // Overwrite the same rows, so the live data stays the same size.
        let mut wb = WriteBatch::new();
        for i in 0..ROWS {
            let v = vec![(round + i) as u8; VALUE];
            let row = format!("row{i:05}");
            wb.put(t.id, f, row.as_bytes(), b"q", None, ValueRef::Bytes(&v))
                .unwrap();
        }
        db.commit(wb, Some(Durability::Buffered)).unwrap();
        db.compact(Some(t.id)).unwrap();
        max_len = max_len.max(file_len(&vfs));
    }
    assert!(db.metrics().compactions >= 100, "{:?}", db.metrics());
    // Live data is ~30 KB of values (one SST in one 64 KiB extent, plus the manifest); allow
    // twice that plus a constant for the manifest, a flush output and a compaction in flight.
    let live = u64::from(ROWS) * VALUE as u64;
    let bound = 2 * live + (2 << 20);
    assert!(
        max_len <= bound,
        "file reached {max_len} bytes over 100 compactions (bound {bound})"
    );
    db.close().unwrap();
}
