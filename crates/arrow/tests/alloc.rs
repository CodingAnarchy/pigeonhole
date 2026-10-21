//! #691: the export allocates per batch, never per cell. A counting global allocator watches
//! a scan exported in batches of a fixed count: its allocator calls must not grow with the
//! cells per batch (256 vs 1024) or with the cells per row (1 vs 32), and stay under a fixed
//! number per batch.
//!
//! The counting allocator needs `unsafe` (`GlobalAlloc`); it lives in this test binary only,
//! as in `pigeonhole`'s `tests/alloc.rs`. The library itself forbids `unsafe`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell as StdCell;
use std::sync::Arc;

use pigeonhole::{Family, Options, Pigeonhole, Table};
use pigeonhole_arrow::{ArrowOptions, scan_to_arrow};
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

/// A table of `rows` rows of `cells` cells each, in the memtable.
fn open(name: &str, rows: usize, cells: usize) -> (Pigeonhole, Table) {
    let vfs = SimVfs::new(1);
    let db = Pigeonhole::open(
        format!("/db/{name}.phdb"),
        Options::default()
            .vfs(Arc::clone(&vfs) as _)
            .shards(1)
            .memtable_budget(32 << 20)
            .wal_segment_size(256 << 10)
            .row_cache(0),
    )
    .expect("open");
    let t = db
        .table("t")
        .expect("table")
        .family("f", Family::default())
        .create_if_missing()
        .expect("create");
    for r in 0..rows {
        let mut m = t.mutate(format!("row{r:05}").as_bytes());
        for c in 0..cells {
            m = m.put("f", format!("q{c:04}").as_bytes(), &[7u8; 24]);
        }
        m.commit().expect("commit");
    }
    (db, t)
}

/// Allocator calls exporting `t` in batches of `batch` cells, after the first batch (which
/// sizes the buffers), and the batches counted.
fn export_allocations(t: &Table, batch: usize) -> (u64, usize) {
    let options = ArrowOptions::default()
        .typed_values(true)
        .row_dictionary(true)
        .batch_cells(batch)
        .batch_bytes(1 << 30);
    let mut export = scan_to_arrow(t.scan_prefix(b""), &options).expect("export");
    drop(export.next().expect("a batch").expect("batch"));
    allocations(|| {
        let mut batches = 0;
        for b in export.by_ref() {
            assert_eq!(b.expect("batch").num_rows(), batch);
            batches += 1;
        }
        batches
    })
}

/// The most allocator calls one batch may make: about two per buffer (the next batch's
/// buffer and the Arrow array around it), the batch and its schema's columns, and their
/// frees. Measured at about 100 on macOS; the point is that it is fixed.
const PER_BATCH: u64 = 160;

#[test]
fn export_allocations_do_not_grow_with_cells() {
    // Nine batches each (the first is not counted): 256 vs 1024 cells a batch, and rows of 1
    // vs 32 cells.
    let mut runs = Vec::new();
    for (name, rows, cells, batch) in [
        ("narrow-small", 9 * 256, 1, 256),
        ("wide-small", 9 * 8, 32, 256),
        ("narrow-large", 9 * 1024, 1, 1024),
        ("wide-large", 9 * 32, 32, 1024),
    ] {
        let (db, t) = open(name, rows, cells);
        let (n, batches) = export_allocations(&t, batch);
        assert_eq!(batches, 8, "{name}");
        eprintln!("{name}: {n} allocator calls for {batches} batches of {batch} cells");
        assert!(
            n <= PER_BATCH * batches as u64,
            "{name}: {n} allocator calls for {batches} batches: more than {PER_BATCH} a batch"
        );
        runs.push(n);
        drop(t);
        db.close().expect("close");
    }
    let [narrow_small, wide_small, narrow_large, wide_large] = runs[..] else {
        unreachable!()
    };
    assert!(
        narrow_large <= narrow_small + 8 && wide_large <= wide_small + 8,
        "allocations grow with cells per batch: {runs:?}"
    );
    assert!(
        wide_small <= narrow_small + 8 && wide_large <= narrow_large + 8,
        "allocations grow with cells per row: {runs:?}"
    );
}
