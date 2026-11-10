# Performance compared with SQLite

Pigeonhole is often weighed against SQLite: both are embedded, single-file stores. This page states the tradeoff for point reads under a mixed read-write load, measured as #759 judges it.

**Setup:** 16 threads on one machine (AMD Ryzen 9 7950X3D, NVMe, Linux 6.8). YCSB A (50% reads, 50% updates) and F (50% reads, 50% read-modify-writes) over 1 million rows. Each store has the same 1 GiB of read cache. Both run at their default durability: Pigeonhole's `Durability::GroupSync`, and SQLite in WAL mode with `synchronous=FULL`. Pigeonhole runs its defaults, including the shard-thread load controller, which is on by default. Measured on 2026-10-29 at commit `cba09184`, three runs of each arm; the ranges below span the runs.

| | SQLite | Pigeonhole, as fast as it goes | Pigeonhole, at SQLite's rate |
|---|--:|--:|--:|
| Operations per second (YCSB A) | 39K | 496K | 39K |
| Operations per second (YCSB F) | 38K | 360K | 38K |
| Row read p50 | 8 µs | 3–4 µs | 2–3 µs |
| Row read p99 | 18–19 µs | 28–31 µs | 26–30 µs |
| Row read p99.9 | 43–47 µs | 41–43 µs | 36–42 µs |
| Write p99.9 | 129 ms | 0.16–0.23 ms | 0.13–0.20 ms |

- **Throughput:** Pigeonhole does 9–13× SQLite's operations per second. SQLite commits through a single writer with an fsync per transaction, so more threads don't make it faster. It reaches its ceiling at 4 threads.
- **Typical reads:** Pigeonhole is 2–4× faster at the median.
- **Read p99:** Pigeonhole's is about 10 µs higher (about 1.4–1.7×), whatever the load. It comes from Pigeonhole's read path for a row held in cache, not from cache misses: a larger cache doesn't remove it (26–28 µs at 5 GiB). Nor does slowing Pigeonhole down to SQLite's rate. 1.0 ships with this gap documented; narrowing it is tracked in [#1069](https://github.com/CodingAnarchy/pigeonhole/issues/1069).
- **The far tail:** Pigeonhole's read p99.9 is slightly lower (42 against 45–46 µs on average). SQLite's writes have a 129 ms p99.9. That's its default busy handler: a writer that finds the database locked sleeps and retries, and the sleeps grow while other writers hold the lock.

If your reads must stay under ~20 µs at the 99th percentile and SQLite's single-writer throughput is enough, SQLite meets that and Pigeonhole doesn't yet. If you need more write throughput, a lower median, or bounded write tails under contention, Pigeonhole is ahead on each.

The benchmark is `phdb-bench` in this repository (`--cache-model total --cache 1073741824`, plus `--offered-rate` for the paced runs; [`docs/bench.md`](../bench.md) has the options). The full results are on [#759](https://github.com/CodingAnarchy/pigeonhole/issues/759); these numbers are its 2026-10-29 re-run on `cba09184`. The 1.0 release gate (2026-10-30, `790ae3a0`) didn't repeat this equal-cache comparison; in its contended runs with a 256 MiB cache, YCSB A's row-read p99 was 37 µs at `Buffered` and 35 µs at `GroupSync`, against 43 and 33 µs on `cba09184`.

## Allocator

Pigeonhole's library crates don't choose a memory allocator: your application's global allocator is the one they use. On Linux with glibc's malloc, that costs throughput on read-heavy work at high thread counts. The cost isn't lock waits so much as glibc's own allocation paths and freeing on a different thread than allocated, which the engine does when it evicts cached blocks and when you drop rows on another thread.

Measured on the AX102 (AMD Ryzen 9 7950X3D, 32 threads, Linux 6.8, glibc 2.39), `phdb-bench` at full scale, two runs per allocator (#1558):

| workload | glibc malloc | jemalloc 5.3 | mimalloc 2.1 |
|---|--:|--:|--:|
| YCSB C (point reads), 32 threads | 3.36–3.38M ops/s | 4.01–4.12M (+21%) | 4.36–4.44M (+31%) |
| Sparse wide rows, 16 threads, `Buffered` | 100–110K | 118–121K (+13%) | 98–100K (−5%) |
| YCSB A, 16 threads, `Buffered` | 1.56–1.58M | 1.69–1.72M (+9%) | 1.68M (+7%) |

- **jemalloc** was faster on every workload, for 0–20% more peak memory. It's what the `phdb` binary uses.
- **mimalloc** was fastest on point reads, but slower on large, growing rows and used up to 30% more memory on YCSB C.
- **glibc's tunables** (a larger thread cache, `GLIBC_TUNABLES=glibc.malloc.tcache_count=1024:glibc.malloc.tcache_max=65536`) made point reads 10% slower. Don't use them for this.

**In a Rust application,** set the global allocator in your binary, not in a library:

```rust,ignore
// Cargo.toml: tikv-jemallocator = "0.7"   (not on Windows)
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

**From Python or Ruby,** the interpreter's allocator applies. On Linux you can preload jemalloc for the process, for example `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 python app.py` (from the `libjemalloc2` package on Debian and Ubuntu). Measure your own workload first: the gain depends on how many threads read at once.

**The `phdb` binary** (CLI, `phdb serve`, `phdb mcp`) is built with jemalloc on every platform but Windows, as the default `jemalloc` feature of `pigeonhole-cli`. Building it needs a C compiler and `make`. To build without it: `cargo install pigeonhole-cli --no-default-features --features embed`.
