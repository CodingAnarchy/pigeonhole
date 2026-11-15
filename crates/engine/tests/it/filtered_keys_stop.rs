//! #1931: a multi-get's filtered jumps stop at each key's row, as #1924's row reads do. A key
//! whose row holds no qualifier the filter keeps sent a source's cursor seeking (or
//! skipping) row by row through the rest of its file: the keys path keeps its sources across
//! keys, so #1930's one-row end didn't reach it. Now a jump that leaves the key's row stops
//! there, on a key the filter didn't admit, and a source that stopped at or past the next
//! key settles from there before the merge reads it. Covered for SST and memtable sources
//! alone (each path's stop and re-settle), for the block reads over a level of many SSTs,
//! and for an async multi-get whose re-settle misses the cache.

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, FamilyOptions, KeySet, PickerOptions, QualifierFilter, ReadSpec, TableInfo, ValueRef,
    WriteBatch,
};
use pigeonhole_io::sim::SimVfs;

const ROWS: u32 = 3000;

/// `(row, qualifier)` of the cells a multi-get read.
type Cells = Vec<(Vec<u8>, Vec<u8>)>;

fn row(i: u32) -> Vec<u8> {
    format!("row{i:06}").into_bytes()
}

/// A 400-byte value that compresses poorly, so blocks hold a few cells each.
fn value(x: &mut u64) -> [u8; 400] {
    let mut v = [0u8; 400];
    for b in &mut v {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *b = *x as u8;
    }
    v
}

fn engine(path: &str, seed: u64, cache: usize) -> (Arc<Engine>, Arc<TableInfo>, Arc<SimVfs>) {
    let vfs = SimVfs::new(seed);
    let mut o = common::options(Arc::clone(&vfs), 1, 4 << 20);
    o.block_cache_bytes = cache;
    o.tablet_changes = false;
    let mut c = PickerOptions::default();
    c.target_sst_bytes = 256 << 10;
    o.compaction = c;
    let db = Engine::open(Path::new(path), o).unwrap();
    let t = db
        .create_table(
            "t",
            &[("f".into(), FamilyOptions::default().max_versions(0))],
        )
        .unwrap();
    (db, t, vfs)
}

/// Writes `cells` (row, qualifier) with a value each, in one commit.
fn put(db: &Engine, t: &TableInfo, x: &mut u64, cells: &[(Vec<u8>, Vec<u8>)]) {
    let f = t.family("f").unwrap().id;
    let mut wb = WriteBatch::new();
    for (r, q) in cells {
        wb.put(t.id, f, r, q, Some(10), ValueRef::Bytes(&value(x)))
            .unwrap();
    }
    db.commit(wb, None).unwrap();
}

fn spec(keep: Option<&[u8]>) -> ReadSpec {
    let mut s = ReadSpec::default();
    s.versions = 0;
    if let Some(p) = keep {
        s.qualifiers = QualifierFilter::Prefix(p.to_vec());
    }
    s
}

/// `(row, qualifier)` of every cell a multi-get of `keys` reads under `keep`, and the block
/// reads it made on this thread: synchronously, or by polling with `poll`'s simulator, whose
/// deferred I/O it completes at each `Pending`.
fn multi_get(
    db: &Engine,
    t: &TableInfo,
    keys: &[Vec<u8>],
    keep: Option<&[u8]>,
    poll: Option<&SimVfs>,
) -> (Cells, u64) {
    let snap = db.snapshot().unwrap();
    let set = KeySet::from_unsorted(keys.iter());
    pigeonhole_sst::counting_file_reads(|| {
        let mut cur = db.multi_get(&snap, t.id, set.clone(), spec(keep)).unwrap();
        let mut got = Vec::new();
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            let next = if let Some(vfs) = poll {
                match cur.poll_next_key(&mut cx) {
                    Poll::Ready(r) => r.unwrap(),
                    Poll::Pending => {
                        vfs.complete_all_io();
                        continue;
                    }
                }
            } else {
                cur.next_key().unwrap()
            };
            let Some(i) = next else { break };
            let r = set.get(i).to_vec();
            cur.counted(|cur| {
                while let Some(c) = cur.next_cell().unwrap() {
                    got.push((r.clone(), c.qualifier.to_vec()));
                }
            });
        }
        got
    })
}

/// Rows `a` and `b`, adjacent: `a` holds only `a1`, `b` holds `a2` and `b1`. Under prefix
/// `b`, key `a`'s seek to its hint leaves its row and stops on `b`'s `a2`, which the filter
/// drops; key `b` then finds its source already there, and must not read `a2`.
fn adjacent(db: &Engine, t: &TableInfo, x: &mut u64) {
    put(
        db,
        t,
        x,
        &[
            (b"a".to_vec(), b"a1".to_vec()),
            (b"b".to_vec(), b"a2".to_vec()),
            (b"b".to_vec(), b"b1".to_vec()),
        ],
    );
}

#[test]
fn a_source_stopped_on_the_next_keys_row_settles_before_it_is_read() {
    // `compact`: the rows in one SST (the SST cursor's stop), or in the memtable alone (the
    // lazy memtable cursor's).
    for compact in [true, false] {
        let path = format!("/db/filtered_keys_stop_adjacent_{compact}.phdb");
        let (db, t, vfs) = engine(&path, 1931, 4 << 20);
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        adjacent(&db, &t, &mut x);
        if compact {
            db.compact(None).unwrap();
        }
        let keys = [b"a".to_vec(), b"b".to_vec()];
        for poll in [None, Some(&*vfs)] {
            let (got, _) = multi_get(&db, &t, &keys, Some(b"b"), poll);
            assert_eq!(
                got,
                vec![(b"b".to_vec(), b"b1".to_vec())],
                "compact {compact}, poll {}",
                poll.is_some()
            );
            // The same through a row skip: prefix `0` sorts before every qualifier here.
            let (got, _) = multi_get(&db, &t, &keys, Some(b"0"), poll);
            assert!(got.is_empty(), "compact {compact}: {got:?}");
        }
        db.close().unwrap();
    }
}

/// Rows `0..ROWS`, one 400-byte cell `q0` each, compacted into a level of 256 KiB SSTs; then
/// rows `ROWS..ROWS + 200` in the memtable. A block cache much smaller than an SST, so a read
/// past its row misses.
fn many(db: &Engine, t: &TableInfo) {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for chunk in (0..ROWS).collect::<Vec<_>>().chunks(100) {
        let cells: Vec<_> = chunk.iter().map(|&i| (row(i), b"q0".to_vec())).collect();
        put(db, t, &mut x, &cells);
    }
    db.compact(None).unwrap();
    let cells: Vec<_> = (ROWS..ROWS + 200)
        .map(|i| (row(i), b"q0".to_vec()))
        .collect();
    put(db, t, &mut x, &cells);
}

#[test]
fn a_multi_get_whose_filter_keeps_nothing_in_its_rows_reads_only_around_them() {
    let (db, t, _) = engine("/db/filtered_keys_stop_many.phdb", 1931, 16 << 10);
    many(&db, &t);
    let keys: Vec<_> = [0, 10, 1500, ROWS - 1, ROWS + 5]
        .into_iter()
        .map(row)
        .collect();
    let (all, _) = multi_get(&db, &t, &keys, None, None);
    assert_eq!(all.len(), keys.len());
    assert_eq!(multi_get(&db, &t, &keys, Some(b"q"), None).0, all);
    let mut most = 0;
    // `~` sorts after `q0` (a seek to the hint), `a` before it (a row skip).
    for keep in [&b"~"[..], b"a"] {
        let (got, reads) = multi_get(&db, &t, &keys, Some(keep), None);
        assert!(got.is_empty(), "prefix {keep:?}: {got:?}");
        most = most.max(reads);
    }
    // At most the blocks around each key's row (2 each here). Without the stop, row 0 and
    // row 10 read on through their SST: 15 to 17 uncached block reads each.
    assert!(
        most <= 4 * keys.len() as u64,
        "a multi-get of {} keys made {most} block reads",
        keys.len()
    );
    db.close().unwrap();
}

#[test]
fn an_async_multi_get_whose_resettle_misses_the_cache_reads_the_right_cells() {
    // Row `a` holds `a1`; row `b` holds 200 cells `a000..a199` (80 KB, over several blocks)
    // and then `b1`. Key `a`'s jump stops on `b`'s `a000`, in the block key `a`'s probe
    // fetched. Key `b`'s re-settle seeks to `b`'s `b` hint, blocks later and not cached: it
    // misses (deferred I/O: a cache-only read of an uncached block doesn't complete in
    // place), the key unwinds, and its retry seeks afresh. The async read runs first, on an
    // emptied cache.
    let (db, t, vfs) = engine("/db/filtered_keys_stop_async.phdb", 1931, 4 << 20);
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut cells = vec![(b"a".to_vec(), b"a1".to_vec())];
    cells.extend((0..200).map(|i| (b"b".to_vec(), format!("a{i:03}").into_bytes())));
    cells.push((b"b".to_vec(), b"b1".to_vec()));
    put(&db, &t, &mut x, &cells);
    db.compact(None).unwrap();
    let keys = [b"a".to_vec(), b"b".to_vec()];
    let want = vec![(b"b".to_vec(), b"b1".to_vec())];
    // Shrinking the cache evicts every block nothing pins; growing it back evicts nothing.
    db.set_cache_capacities(1, 0);
    db.set_cache_capacities(4 << 20, 0);
    vfs.set_deferred_io(true);
    let polled = multi_get(&db, &t, &keys, Some(b"b"), Some(&vfs)).0;
    vfs.set_deferred_io(false);
    assert_eq!(polled, want);
    assert_eq!(multi_get(&db, &t, &keys, Some(b"b"), None).0, want);
    db.close().unwrap();
}
