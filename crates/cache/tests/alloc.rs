//! The hit path allocates nothing: a counting global allocator watches `get`, reading the
//! bytes, cloning into a `Cell`, and dropping every handle.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell as StdCell;

use pigeonhole_cache::{BlockCache, BlockKey, Cell, Priority, RowCache};

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
fn allocations(f: impl FnOnce()) -> u64 {
    let before = ALLOCS.with(StdCell::get);
    f();
    ALLOCS.with(StdCell::get) - before
}

#[test]
fn block_cache_hit_path_allocates_nothing() {
    let cache = BlockCache::new(64 << 20, 0);
    let keys: Vec<BlockKey> = (0..1000)
        .map(|i| BlockKey {
            file: i % 5,
            offset: i * 4096,
        })
        .collect();
    for k in &keys {
        drop(cache.insert(*k, vec![k.offset as u8; 4096].into(), Priority::Normal));
    }
    let n = allocations(|| {
        let mut sum = 0u64;
        for k in &keys {
            let h = cache.get(*k).expect("hit");
            sum += u64::from(h[0]);
            let cell = Cell::in_block(h.clone(), 100..200);
            let cell2 = cell.clone();
            sum += u64::from(cell2.bytes()[0]);
            drop((h, cell, cell2));
        }
        std::hint::black_box(sum);
    });
    assert_eq!(n, 0, "the block-cache hit path allocated {n} times");
}

#[test]
fn block_cache_miss_allocates_nothing() {
    let cache = BlockCache::new(1 << 20, 0);
    let misses = || {
        for i in 0..100 {
            assert!(cache.get(BlockKey { file: 9, offset: i }).is_none());
        }
    };
    // Warm up: on platforms without futexes (macOS), std's `Mutex` boxes its pthread mutex
    // on first lock, a one-time allocation per shard.
    misses();
    let n = allocations(misses);
    assert_eq!(n, 0);
}

#[test]
fn row_cache_hit_path_allocates_nothing() {
    let rows = RowCache::new(16 << 20);
    let names: Vec<Vec<u8>> = (0..500u32)
        .map(|i| format!("user:{i}").into_bytes())
        .collect();
    for (i, r) in names.iter().enumerate() {
        drop(rows.insert(i as u64 % 4, r, 1, vec![i as u8; 64]));
    }
    let n = allocations(|| {
        let mut sum = 0u64;
        for (i, r) in names.iter().enumerate() {
            let h = rows.get(i as u64 % 4, r, 1).expect("hit");
            sum += u64::from(h[0]);
            assert!(rows.get(i as u64 % 4, r, 2).is_none());
        }
        std::hint::black_box(sum);
    });
    assert_eq!(n, 0, "the row-cache hit path allocated {n} times");
}

#[test]
fn counter_sees_allocations() {
    let n = allocations(|| drop(std::hint::black_box(vec![0u8; 16])));
    assert_eq!(n, 2);
}
