//! #712, D293: a scan whose row limit leaves it short starts its readahead ramp only after 8
//! forward blocks, so one that is short in blocks too never waits on a fetch it just
//! submitted, and one that is long in blocks (a wide row, a run of deleted rows) still reads
//! ahead past them; a scan with a larger limit, or none, keeps the usual ramp. Counted on the
//! scanning thread (`pigeonhole_sst::counting_readahead`).

use crate::common;

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

/// An empty engine with a block cache too small to hold what the tests write, so a scan
/// misses.
fn empty() -> (Arc<Engine>, Arc<pigeonhole_engine::TableInfo>) {
    let vfs = SimVfs::new(712);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.block_cache_bytes = 64 << 10;
    o.tablet_changes = false;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    (db, t)
}

/// Writes `cells` qualifiers to each row of `rows`, 100 cells a commit, each 400 bytes that
/// do not compress (so the blocks they fill are the blocks a scan reads).
fn write(db: &Engine, t: &pigeonhole_engine::TableInfo, rows: std::ops::Range<u32>, cells: u32) {
    let f = t.family("f").unwrap().id;
    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ u64::from(rows.start);
    let mut value = vec![0u8; 400];
    let all: Vec<(u32, u32)> = rows.flat_map(|i| (0..cells).map(move |q| (i, q))).collect();
    for chunk in all.chunks(100) {
        let mut wb = WriteBatch::new();
        for &(i, q) in chunk {
            let q = format!("q{q:05}");
            for b in &mut value {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *b = x as u8;
            }
            wb.put(
                t.id,
                f,
                &row(i),
                q.as_bytes(),
                None,
                ValueRef::Bytes(&value),
            )
            .unwrap();
        }
        db.commit(wb, None).unwrap();
    }
}

/// An engine whose rows (one 400-byte cell each) are all in SSTs (many data blocks).
fn open() -> (Arc<Engine>, Arc<pigeonhole_engine::TableInfo>) {
    let (db, t) = empty();
    write(&db, &t, 0..ROWS, 1);
    db.compact(None).unwrap();
    (db, t)
}

/// Scans from row `from` with `limit` (0: none), reading every cell, as ycsb-e does; returns
/// the rows read and the blocks the scan's readahead submitted.
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
            while c.next_cell().unwrap().is_some() {}
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

#[test]
fn a_short_scan_into_a_wide_row_reads_ahead_once_past_the_bound() {
    // Row 100 holds 2000 cells (about 50 blocks): one row, but long in blocks.
    let (db, t) = empty();
    write(&db, &t, 0..100, 1);
    write(&db, &t, 100..101, 2000);
    write(&db, &t, 101..200, 1);
    db.compact(None).unwrap();
    let (rows, issued) = scan(&db, &t, 100, 1);
    assert_eq!(rows, 1);
    assert!(
        issued > 0,
        "a limit-1 scan of a 50-block row read nothing ahead"
    );
    db.close().unwrap();
}

#[test]
fn a_short_scan_across_deleted_rows_reads_ahead_once_past_the_bound() {
    // Rows 100..2900 are deleted in a newer SST than the one holding them, so a limit-1 scan
    // from row 100 walks every block of them before its one row.
    let (db, t) = open();
    for chunk in (100..2900).collect::<Vec<u32>>().chunks(100) {
        let mut wb = WriteBatch::new();
        for &i in chunk {
            wb.delete_row(t.id, &row(i), None).unwrap();
        }
        db.commit(wb, None).unwrap();
    }
    db.flush().unwrap();
    let (rows, issued) = scan(&db, &t, 100, 1);
    assert_eq!(rows, 1);
    assert!(
        issued > 0,
        "a limit-1 scan across 2800 deleted rows read nothing ahead"
    );
    db.close().unwrap();
}
