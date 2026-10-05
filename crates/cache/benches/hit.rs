//! Hit-path latency of the block and row caches, single-threaded and with every core
//! hammering the same cache. A counting allocator asserts the measured loops allocate
//! nothing (the same check as `tests/alloc.rs`, here over criterion's millions of iterations).

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use pigeonhole_cache::{BlockCache, BlockKey, Cell, Priority, RowCache};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: forwards every call to `System` unchanged; only counts allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract, which we pass through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator, i.e. from `System`, with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const BLOCKS: u64 = 4096;

fn block_cache() -> BlockCache {
    let cache = BlockCache::new(64 << 20, 0);
    for i in 0..BLOCKS {
        let k = BlockKey {
            file: i % 16,
            offset: i * 4096,
        };
        drop(cache.insert(k, vec![i as u8; 4096].into(), Priority::Normal));
    }
    cache
}

/// Runs `f` and panics if it allocated (criterion's own bookkeeping runs outside `f`).
fn no_alloc<R>(f: impl FnOnce() -> R) -> R {
    let before = ALLOCS.load(Ordering::Relaxed);
    let r = f();
    let n = ALLOCS.load(Ordering::Relaxed) - before;
    assert_eq!(n, 0, "hit path allocated {n} times");
    r
}

fn bench(c: &mut Criterion) {
    let cache = block_cache();
    c.bench_function("block_cache/get hit + read + drop", |b| {
        let mut i = 0u64;
        b.iter_custom(|iters| {
            no_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    i = (i + 1) % BLOCKS;
                    let h = cache
                        .get(BlockKey {
                            file: i % 16,
                            offset: i * 4096,
                        })
                        .unwrap();
                    black_box(h[17]);
                }
                start.elapsed()
            })
        })
    });

    c.bench_function("block_cache/get hit -> Cell", |b| {
        let mut i = 0u64;
        b.iter_custom(|iters| {
            no_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    i = (i + 1) % BLOCKS;
                    let h = cache
                        .get(BlockKey {
                            file: i % 16,
                            offset: i * 4096,
                        })
                        .unwrap();
                    let cell = Cell::in_block(h, 64..128);
                    black_box(cell.bytes()[0]);
                }
                start.elapsed()
            })
        })
    });

    c.bench_function("block_cache/get miss", |b| {
        let mut i = 0u64;
        b.iter_custom(|iters| {
            no_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    i += 1;
                    black_box(
                        cache
                            .get(BlockKey {
                                file: 99,
                                offset: i,
                            })
                            .is_none(),
                    );
                }
                start.elapsed()
            })
        })
    });

    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let shared = Arc::new(block_cache());
    c.bench_function(
        &format!("block_cache/get hit, {threads} threads (per op)"),
        |b| {
            b.iter_custom(|iters| {
                let per = iters.div_ceil(threads as u64);
                let handles: Vec<_> = (0..threads)
                    .map(|t| {
                        let cache = Arc::clone(&shared);
                        std::thread::spawn(move || {
                            let start = Instant::now();
                            let mut i = t as u64 * 977;
                            for _ in 0..per {
                                i = (i + 1) % BLOCKS;
                                let h = cache
                                    .get(BlockKey {
                                        file: i % 16,
                                        offset: i * 4096,
                                    })
                                    .unwrap();
                                black_box(h[17]);
                            }
                            start.elapsed()
                        })
                    })
                    .collect();
                let total: Duration = handles.into_iter().map(|h| h.join().unwrap()).sum();
                // Mean per-op latency seen by one thread, scaled to `iters` ops.
                total
                    .div_f64((threads as u64 * per) as f64)
                    .mul_f64(iters as f64)
            })
        },
    );

    let rows = RowCache::new(16 << 20);
    let names: Vec<Vec<u8>> = (0..4096u32)
        .map(|i| format!("user:{i:08}").into_bytes())
        .collect();
    for (i, r) in names.iter().enumerate() {
        drop(rows.insert(1, r, 7, vec![i as u8; 128]));
    }
    c.bench_function("row_cache/get hit", |b| {
        let mut i = 0usize;
        b.iter_custom(|iters| {
            no_alloc(|| {
                let start = Instant::now();
                for _ in 0..iters {
                    i = (i + 1) % names.len();
                    black_box(rows.get(1, &names[i], 7).unwrap()[0]);
                }
                start.elapsed()
            })
        })
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
