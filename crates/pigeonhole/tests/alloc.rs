//! The public read paths add no allocation per cell: a counting global allocator watches
//! `Table::get` (small values copied inline, large ones pinned in the memtable), reading the
//! value and `to_owned`, which together must make exactly the allocator calls of the engine
//! get they wrap; and a scan through the lending cursor `RowIter::next_ref`, whose
//! allocations must not grow with the number of cells per row.
//!
//! The counting allocator needs `unsafe` (`GlobalAlloc`); it lives in this test binary only,
//! as in `pigeonhole-cache`'s `tests/alloc.rs`. The library itself forbids `unsafe`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell as StdCell;
use std::path::Path;
use std::sync::Arc;

use pigeonhole::{Family, Options, Pigeonhole, Table};
use pigeonhole_engine::{
    Engine, EngineOptions, FamilyOptions, ValueRef, WriteBatch as EngineBatch,
};
use pigeonhole_io::sim::SimVfs;

struct Counting;

thread_local! {
    static ALLOCS: StdCell<u64> = const { StdCell::new(0) };
}

fn bump() {
    // `try_with`: the allocator can run during thread-local teardown.
    let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: forwards every call to `System` unchanged; only counts calls on this thread.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract, which we pass through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        bump();
        // SAFETY: as for `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump();
        // SAFETY: as for `alloc`; `ptr` came from this allocator, i.e. from `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        bump();
        // SAFETY: `ptr` came from this allocator, i.e. from `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocator calls (alloc, realloc and dealloc) made by `f` on this thread.
fn allocations<R>(f: impl FnOnce() -> R) -> (u64, R) {
    let before = ALLOCS.with(StdCell::get);
    let r = f();
    (ALLOCS.with(StdCell::get) - before, r)
}

fn open(name: &str) -> (Pigeonhole, Table) {
    let vfs = SimVfs::new(1);
    let db = Pigeonhole::open(
        format!("/db/{name}.phdb"),
        Options::default()
            .vfs(Arc::clone(&vfs) as _)
            .shards(1)
            .memtable_budget(8 << 20)
            .wal_segment_size(256 << 10)
            // The budgets are the read and write paths' own (D216 turned the row cache on
            // by default; a read that fills it allocates the copy it keeps).
            .row_cache(0),
    )
    .expect("open");
    let t = db
        .table("t")
        .expect("table")
        .family("f", Family::default())
        .family("g", Family::default())
        .create_if_missing()
        .expect("create");
    (db, t)
}

#[test]
fn get_adds_no_allocation_to_the_engine_get() {
    // The same data in a public database and in an engine opened directly.
    let (db, t) = open("get");
    let mut o = EngineOptions::new(SimVfs::new(1));
    o.create_if_missing = true;
    o.shards = 1;
    o.memtable_budget = 8 << 20;
    let engine = Engine::open(Path::new("/db/engine.phdb"), o).expect("open engine");
    let info = engine
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .expect("create");
    let (table, f) = (info.id, info.families[0].id);
    // 16 and 100 bytes are copied inline; 1000 bytes stay pinned in the memtable arena.
    for (i, len) in [16usize, 100, 1000].into_iter().enumerate() {
        let row = format!("row{i}");
        let value = vec![i as u8 + 1; len];
        t.mutate(row.as_bytes())
            .put("f", b"q", &value)
            .commit()
            .expect("commit");
        let mut wb = EngineBatch::new();
        wb.put(
            table,
            f,
            row.as_bytes(),
            b"q",
            None,
            ValueRef::Bytes(&value),
        )
        .expect("put");
        engine.commit(wb, None).expect("commit");
    }
    for i in 0..3 {
        let row = format!("row{i}").into_bytes();
        for q in [&b"q"[..], b"missing"] {
            // Warm up, then compare the public get with the engine call it wraps.
            let _ = t.get(&row, "f", q).expect("get");
            let _ = engine.get_latest(table, f, &row, q).expect("get");
            let (engine_n, _) = allocations(|| engine.get_latest(table, f, &row, q).expect("get"));
            let (public_n, len) = allocations(|| {
                let c = t.get(&row, "f", q).expect("get");
                let len = c
                    .as_ref()
                    .map_or(0, |c| c.value().len() as u64 + c.timestamp() % 2);
                // Reading, owning and dropping the cell allocate nothing either.
                let owned = c.as_ref().map(|c| c.to_owned());
                len + owned.map_or(0, |o| o.value().len() as u64)
            });
            assert_eq!(len > 0, q == b"q");
            assert_eq!(
                public_n,
                engine_n,
                "row{i}/{}: Table::get made {public_n} allocator calls, the engine get {engine_n}",
                String::from_utf8_lossy(q)
            );
        }
    }
    drop(t);
    db.close().expect("close");
    engine.close().expect("close engine");
}

/// Allocations of a scan through `next_ref` after its first row (which sizes the
/// iterator's buffers), and the cells seen in those rows.
fn scan_allocations(t: &Table) -> (u64, usize) {
    let mut it = t.scan_prefix(b"").iter().expect("iter");
    assert!(it.next_ref().expect("next").is_some());
    allocations(|| {
        let mut cells = 0;
        while let Some(row) = it.next_ref().expect("next") {
            for e in row.iter() {
                cells += e.cell.value().len().min(1) + e.qualifier.len().min(1);
            }
        }
        cells
    })
}

#[test]
fn lending_scan_allocations_do_not_grow_with_cells() {
    let rows = 50usize;
    let mut per_row = Vec::new();
    for cells in [1usize, 32] {
        let (db, t) = open(&format!("scan{cells}"));
        for r in 0..rows {
            let mut m = t.mutate(format!("row{r:04}").as_bytes());
            for c in 0..cells {
                m = m.put("f", format!("q{c:04}").as_bytes(), &[7u8; 24]);
            }
            m.commit().expect("commit");
        }
        let (n, seen) = scan_allocations(&t);
        assert_eq!(seen, (rows - 1) * cells * 2);
        eprintln!(
            "{cells} cells per row: {n} allocations for {} rows",
            rows - 1
        );
        per_row.push(n);
        drop(t);
        db.close().expect("close");
    }
    assert!(
        per_row[1] <= per_row[0],
        "allocations grow with cells per row: {per_row:?} (1 vs 32 cells, {rows} rows)"
    );
}

/// Allocator calls of one `RowRead::read` (after a warm-up), reading every cell's value and
/// qualifier, for a row of `cells` cells.
fn row_read_allocations(t: &Table, cells: usize) -> u64 {
    let row = format!("wide{cells:04}");
    let mut m = t.mutate(row.as_bytes());
    for c in 0..cells {
        m = m.put("f", format!("q{c:04}").as_bytes(), &[7u8; 24]);
    }
    m.commit().expect("commit");
    let read = || {
        let r = t.row(row.as_bytes()).read().expect("read").expect("row");
        r.iter()
            .map(|e| e.cell.value().len() + e.qualifier.len())
            .sum::<usize>()
    };
    let _ = read();
    let (n, seen) = allocations(read);
    assert!(seen > 0);
    n
}

/// #287 item 1: a row read builds the row it returns directly (no intermediate row, no
/// second copy of each cell). Its allocations are a constant per read plus the row
/// buffers' growth, which is logarithmic in the cells.
#[test]
fn row_read_allocations_per_cell() {
    let (db, t) = open("row_read");
    let one = row_read_allocations(&t, 1);
    let wide = row_read_allocations(&t, 33);
    let per_cell = (wide - one) as f64 / 32.0;
    eprintln!(
        "row read: {one} allocator calls for 1 cell, {wide} for 33 ({per_cell:.2} per extra cell)"
    );
    assert!(
        one <= ROW_READ_BUDGET.0,
        "{one} allocator calls for a 1-cell row"
    );
    assert!(
        per_cell <= ROW_READ_BUDGET.1,
        "{per_cell:.2} allocator calls per extra cell"
    );
    drop(t);
    db.close().expect("close");
}

/// `(allocator calls for a 1-cell row read, per extra cell)`; lower them when a change
/// improves the path.
const ROW_READ_BUDGET: (u64, f64) = (10, 0.5);

/// D299: a multi-get lends every row from one reused buffer and reuses each tablet's read
/// setup, so past its first rows it allocates nothing per row. Allocator calls of 200 rows
/// after 20, against 2,000 rows after 20, with the blocks cached: the same.
#[test]
fn multi_get_allocations_do_not_grow_with_rows() {
    let (db, t) = open("multi_get");
    for r in 0..3_000u32 {
        t.mutate(format!("r{r:05}").as_bytes())
            .put("f", b"q", &[7u8; 24])
            .put("g", b"q", &[7u8; 24])
            .commit()
            .expect("commit");
    }
    db.flush().expect("flush");
    // A first pass reads every block into the cache: a miss allocates its block.
    let all =
        pigeonhole::KeySet::from_unsorted((0..3_000u32).map(|i| format!("r{i:05}").into_bytes()));
    let mut warm = t.keys(&all).read().expect("keys");
    while warm.next().expect("next").is_some() {}
    drop(warm);
    let mut calls = Vec::new();
    for n in [200u32, 2_000] {
        let keys = pigeonhole::KeySet::from_unsorted(
            (0..n + 20).map(|i| format!("r{:05}", i).into_bytes()),
        );
        let mut rows = t.keys(&keys).read().expect("keys");
        for _ in 0..20 {
            rows.next().expect("next").expect("a row");
        }
        let (c, seen) = allocations(|| {
            let mut seen = 0;
            for _ in 0..n {
                let (_, row) = rows.next().expect("next").expect("a row");
                seen += row.len();
            }
            seen
        });
        assert_eq!(seen, n as usize * 2);
        eprintln!("multi-get: {c} allocator calls for {n} rows past the first 20");
        calls.push(c);
    }
    assert!(
        calls[1] <= calls[0] + 4,
        "a multi-get's allocations grow with rows: {calls:?} (200 vs 2,000 rows)"
    );
    drop(t);
    db.close().expect("close");
}

/// Writes a row of `cells` cells (24-byte values) in commits of 1,000.
fn wide_row(t: &Table, row: &[u8], cells: usize) {
    for chunk in (0..cells).collect::<Vec<_>>().chunks(1_000) {
        let mut m = t.mutate(row);
        for &c in chunk {
            m = m.put("f", format!("q{c:06}").as_bytes(), &[7u8; 24]);
        }
        m.commit().expect("commit");
    }
}

/// D297: walking a row cell by cell allocates nothing per cell. The allocator calls of a
/// whole walk (setup included) are the same for a row of 100 cells and one of 10,000.
#[test]
fn row_cells_allocations_do_not_grow_with_cells() {
    let (db, t) = open("row_cells");
    let mut calls = Vec::new();
    for cells in [100usize, 10_000] {
        let row = format!("hub{cells}");
        wide_row(&t, row.as_bytes(), cells);
        let walk = || {
            let mut it = t.row(row.as_bytes()).family("f").cells().expect("cells");
            let mut seen = 0;
            while let Some(e) = it.next().expect("next") {
                seen += e.cell.value().len().min(1) + e.qualifier.len().min(1);
            }
            seen
        };
        let _ = walk();
        let (n, seen) = allocations(walk);
        assert_eq!(seen, cells * 2);
        eprintln!("cells(): {n} allocator calls for a {cells}-cell row");
        calls.push(n);
    }
    assert!(
        calls[1] <= calls[0] + 4,
        "a cell walk's allocations grow with cells: {calls:?} (100 vs 10,000 cells)"
    );
    drop(t);
    db.close().expect("close");
}

/// D297: past its first page, a page walk allocates nothing per page. The pages measured
/// are the second to the ninth: the tenth reaches the row's end, where the walk drops its
/// scan (teardown, not per-page work).
#[test]
fn row_pages_allocate_nothing_past_the_first_page() {
    let (db, t) = open("row_pages");
    wide_row(&t, b"hub", 10_000);
    let mut pages = t.row(b"hub").family("f").pages(1_000).expect("pages");
    assert_eq!(pages.next().expect("next").expect("page").len(), 1_000);
    let (n, count) = allocations(|| {
        let mut count = 0;
        for _ in 0..8 {
            let page = pages.next().expect("next").expect("a page");
            count += page.iter().filter(|e| !e.cell.value().is_empty()).count();
        }
        count
    });
    assert_eq!(count, 8_000);
    eprintln!("pages(1000): {n} allocator calls for 8 pages past the first");
    assert_eq!(n, 0, "{n} allocator calls for 8 pages past the first");
    assert_eq!(
        pages.next().expect("next").expect("the last page").len(),
        1_000
    );
    assert!(pages.next().expect("next").is_none());
    drop(pages);
    drop(t);
    db.close().expect("close");
}
