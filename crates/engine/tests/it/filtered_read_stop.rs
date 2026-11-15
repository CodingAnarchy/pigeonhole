//! #1924: a read whose qualifier filter keeps nothing in its row ends at the row. Before, each
//! SST source's seek to the filter's hint (or row skip) landed on the next row, which
//! sent it seeking again, row by row through the rest of its file (and a lone memtable
//! source through its rows): a text search for an id prefix with no documents cost more than
//! one without the prefix. Covered for a row read and for a one-row scan (the text crate's
//! posting reads), over a level of many SSTs with memtable rows after them, counting the
//! block reads on the reading thread (`pigeonhole_sst::counting_file_reads`). A filter that
//! keeps the row's columns reads what an unfiltered read does.

use crate::common;

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, FamilyOptions, PickerOptions, QualifierFilter, ReadSpec, ScanSpec, ValueRef, WriteBatch,
};
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/filtered_read_stop.phdb";
const ROWS: u32 = 3000;

fn row(i: u32) -> Vec<u8> {
    format!("row{i:06}").into_bytes()
}

/// Rows `0..ROWS`, one 400-byte cell `q0` each (row 7 with three versions of `q0` and a
/// deleted `q1`), compacted into a level of 256 KiB SSTs; then rows `ROWS..ROWS + 200` in the
/// memtable. A block cache much smaller than an SST, so a read past its row misses.
fn open() -> (Arc<Engine>, Arc<pigeonhole_engine::TableInfo>) {
    let vfs = SimVfs::new(1924);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.block_cache_bytes = 16 << 10;
    o.tablet_changes = false;
    let mut c = PickerOptions::default();
    c.target_sst_bytes = 256 << 10;
    o.compaction = c;
    let db = Engine::open(Path::new(DB), o).unwrap();
    let t = db
        .create_table(
            "t",
            &[("f".into(), FamilyOptions::default().max_versions(0))],
        )
        .unwrap();
    let f = t.family("f").unwrap().id;
    let mut value = [0u8; 400];
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut put = |wb: &mut WriteBatch, i: u32, q: &[u8], ts: u64| {
        for b in &mut value {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        wb.put(t.id, f, &row(i), q, Some(ts), ValueRef::Bytes(&value))
            .unwrap();
    };
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(100) {
        let mut wb = WriteBatch::new();
        for &i in chunk {
            put(&mut wb, i, b"q0", 10);
        }
        db.commit(wb, None).unwrap();
    }
    let mut wb = WriteBatch::new();
    put(&mut wb, 7, b"q0", 20);
    put(&mut wb, 7, b"q0", 30);
    put(&mut wb, 7, b"q1", 10);
    db.commit(wb, None).unwrap();
    let mut wb = WriteBatch::new();
    wb.delete_column(t.id, f, &row(7), b"q1", None).unwrap();
    db.commit(wb, None).unwrap();
    db.compact(None).unwrap();
    let mut wb = WriteBatch::new();
    for i in ROWS..ROWS + 200 {
        put(&mut wb, i, b"q0", 10);
    }
    db.commit(wb, None).unwrap();
    (db, t)
}

/// `(qualifier, timestamp)` of every cell row `i` reads under `keep` (`None`: every
/// qualifier), all versions, by a row read (`scan` false) or a one-row scan; and the block
/// reads it made.
fn read(
    db: &Engine,
    t: &pigeonhole_engine::TableInfo,
    i: u32,
    keep: Option<&[u8]>,
    scan: bool,
) -> (Vec<(Vec<u8>, u64)>, u64) {
    let snap = db.snapshot().unwrap();
    let mut spec = ReadSpec::default();
    spec.versions = 0;
    if let Some(p) = keep {
        spec.qualifiers = QualifierFilter::Prefix(p.to_vec());
    }
    pigeonhole_sst::counting_file_reads(|| {
        let mut got = Vec::new();
        if scan {
            let mut s = ScanSpec::new(Bound::Included(row(i)), Bound::Included(row(i)));
            s.read = spec;
            s.limit = 1;
            let mut c = db.scan(&snap, t.id, s).unwrap();
            while c.next_row().unwrap() {
                while let Some(cell) = c.next_cell().unwrap() {
                    got.push((cell.qualifier.to_vec(), cell.ts));
                }
            }
        } else if let Some(r) = db.read_row(&snap, t.id, &row(i), &spec).unwrap() {
            for c in &r.cells {
                got.push((r.qualifier(c).to_vec(), c.data.timestamp()));
            }
        }
        got
    })
}

#[test]
fn a_read_whose_filter_keeps_nothing_in_its_row_ends_at_the_row() {
    let (db, t) = open();
    for scan in [false, true] {
        let mut most = 0;
        for i in [0, 7, 10, 1500, ROWS - 1, ROWS + 5] {
            // `~` sorts after `q0` (a seek to the hint), `a` before it (a row skip).
            for keep in [&b"~"[..], b"a"] {
                let (got, reads) = read(&db, &t, i, Some(keep), scan);
                assert!(got.is_empty(), "row {i}, prefix {keep:?}, scan {scan}");
                most = most.max(reads);
            }
            // A prefix that keeps the row's columns: what an unfiltered read returns,
            // versions and the deleted column included.
            let (all, _) = read(&db, &t, i, None, scan);
            assert!(!all.is_empty(), "row {i}, scan {scan}");
            assert_eq!(
                read(&db, &t, i, Some(b"q"), scan).0,
                all,
                "row {i}, scan {scan}"
            );
        }
        // Row 7: three versions of `q0`, `q1` deleted.
        let (seven, _) = read(&db, &t, 7, Some(b"q"), scan);
        assert_eq!(
            seven,
            vec![
                (b"q0".to_vec(), 30),
                (b"q0".to_vec(), 20),
                (b"q0".to_vec(), 10)
            ],
            "scan {scan}"
        );
        // At most the blocks around its row, whatever follows it (2 here). Without the fix,
        // a row early in its SST read on through it: 15 to 17 uncached block reads.
        assert!(most <= 4, "scan {scan}: a read made {most} block reads");
    }
    db.close().unwrap();
}
