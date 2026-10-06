//! A point lookup on hot blocks allocates nothing: a counting global allocator watches the
//! engine's per-get sequence (filter probes, a fresh cursor, a seek, a pinned value cell).
//!
//! The counting allocator needs `unsafe` (`GlobalAlloc`); it lives in this test binary only,
//! as in `pigeonhole-cache`'s `tests/alloc.rs`. The library itself forbids `unsafe`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell as StdCell;
use std::sync::Arc;

use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_format::filter::{column_hash, row_hash};
use pigeonhole_format::key::{Kind, SUFFIX_LEN, encode_key, row_prefix_len};
use pigeonhole_format::manifest::FamilyOptions;
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
use pigeonhole_io::sim::SimVfs;
use pigeonhole_io::{OpenOptions, Vfs};
use pigeonhole_sst::{ReadOptions, ScanFilter, SstReader, SstWriter, SstWriterOptions};

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

/// Allocator calls made by `f` on this thread.
fn allocations(f: impl FnOnce()) -> u64 {
    let before = ALLOCS.with(StdCell::get);
    f();
    ALLOCS.with(StdCell::get) - before
}

#[test]
fn hot_point_lookup_allocates_nothing() {
    let vfs = SimVfs::new(1);
    let file = vfs
        .open("/db".as_ref(), OpenOptions::read_write_create())
        .unwrap();
    let options = SstWriterOptions::for_family(
        &FamilyOptions::default(),
        TableId(1),
        FamilyId(1),
        TabletId(1),
    );
    let extent = ExtentRef {
        page: 1024,
        size_class: 6,
    };
    let mut w = SstWriter::new(file.clone(), extent, SstId(1), options);
    let n = if cfg!(miri) { 200 } else { 20_000u32 };
    let mut keys = Vec::new();
    for i in 0..n {
        let mut k = Vec::new();
        let row = format!("user:{:08}", i / 4);
        encode_key(
            &mut k,
            row.as_bytes(),
            &[b'c', (i % 4) as u8],
            7,
            u64::from(i) + 1,
            Kind::Put,
        )
        .unwrap();
        w.add(&k, format!("\x00value-{i}").as_bytes()).unwrap();
        keys.push(k);
    }
    let meta = w.finish().unwrap();
    let cache = Arc::new(BlockCache::new(64 << 20, 0));
    let sst = Arc::new(SstReader::open(file, &meta, cache, Priority::Normal).unwrap());

    let lookups = || {
        let mut sum = 0u64;
        for k in &keys {
            let row = &k[..row_prefix_len(k).unwrap() - 2];
            let column = &k[..k.len() - SUFFIX_LEN];
            assert!(sst.may_contain_row(row_hash(row)));
            assert!(sst.may_contain_column(column_hash(column)));
            let mut it = sst.iter(ScanFilter::all(), ReadOptions::default());
            it.seek(column).unwrap();
            assert_eq!(it.key(), &k[..]);
            let cell = it.value_cell();
            sum += cell.len() as u64;
            drop((it, cell));
        }
        std::hint::black_box(sum);
    };
    // Warm up: fill the cache (and on macOS, let std's mutexes box their pthread mutex).
    lookups();
    let count = allocations(lookups);
    assert_eq!(count, 0, "hot point lookups allocated {count} times");
}

#[test]
fn counter_sees_allocations() {
    let n = allocations(|| drop(std::hint::black_box(vec![0u8; 16])));
    assert_eq!(n, 2);
}
