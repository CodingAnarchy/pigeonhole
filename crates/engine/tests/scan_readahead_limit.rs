//! #712: a scan whose row limit leaves it short reads no blocks ahead, so it never waits on a
//! fetch it just submitted; a scan with a larger limit, or none, keeps the readahead ramp.
//! Counted on the scanning thread (`pigeonhole_sst::counting_readahead`).

mod common;

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{Engine, FamilyOptions, ScanSpec, ValueRef, WriteBatch};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/scan_readahead_limit.phdb";
const ROWS: u32 = 3000;

fn row(i: u32) -> Vec<u8> {
    format!("row{i:06}").into_bytes()
}

/// An engine whose rows are all in SSTs (many data blocks) and a block cache too small to
/// hold them, so a scan misses.
fn open() -> (Arc<Engine>, Arc<pigeonhole_engine::TableInfo>) {
    let vfs = SimVfs::new(712);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.block_cache_bytes = 64 << 10;
    o.tablet_changes = false;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.family("f").unwrap().id;
    let value = vec![7u8; 400];
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(100) {
        let mut wb = WriteBatch::new();
        for &i in chunk {
            wb.put(t.id, f, &row(i), b"q", None, ValueRef::Bytes(&value))
                .unwrap();
        }
        db.commit(wb, None).unwrap();
    }
    db.compact(None).unwrap();
    (db, t)
}

/// Scans from row `from` with `limit` (0: none); returns the rows read and the blocks the
/// scan's readahead submitted.
fn scan(db: &Engine, t: &pigeonhole_engine::TableInfo, from: u32, limit: u64) -> (u64, u64) {
    let snap = db.snapshot().unwrap();
    let start = row(from);
    let mut spec = ScanSpec::new(Bound::Included(start), Bound::Unbounded);
    spec.limit = limit;
    let (rows, counts) = pigeonhole_sst::counting_readahead(|| {
        let mut c = db.scan(&snap, t.id, spec).unwrap();
        let mut rows = 0;
        while c.next_row().unwrap() {
            rows += 1;
        }
        rows
    });
    (rows, counts.issued)
}

#[test]
fn a_scan_its_limit_leaves_short_reads_no_blocks_ahead() {
    let (db, t) = open();
    for limit in [1, 50, 128] {
        let (rows, issued) = scan(&db, &t, 100, limit);
        assert_eq!(rows, limit);
        assert_eq!(issued, 0, "a scan of at most {limit} rows read ahead");
    }
    db.close().unwrap();
}

#[test]
fn a_longer_or_unlimited_scan_still_reads_ahead() {
    let (db, t) = open();
    let (rows, issued) = scan(&db, &t, 100, 1000);
    assert_eq!(rows, 1000);
    assert!(issued > 0, "a 1000-row scan read nothing ahead");
    let (rows, issued) = scan(&db, &t, 1500, 0);
    assert_eq!(rows, u64::from(ROWS - 1500));
    assert!(issued > 0, "an unlimited scan read nothing ahead");
    db.close().unwrap();
}
