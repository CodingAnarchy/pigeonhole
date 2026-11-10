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

## Caller-runs: an opt-in for buffered put tails

Each shard's writes run on that shard's thread. A thread waiting on its commit hands the work over and waits for the shard's thread to run it. When the application's threads keep every CPU busy, the shard's thread can wait for a scheduler slice before it runs, and that wait becomes the put's tail latency.

`Options::caller_runs(true)` lets the waiting thread run the shard's work itself whenever no other thread is running that shard. A shard is still run by one thread at a time. It's off by default.

```rust
use pigeonhole::{Durability, Options, Pigeonhole};

let db = Pigeonhole::open(
    "data.phdb",
    Options::default()
        .durability(Durability::Buffered)
        .caller_runs(true),
)?;
```

**What it applies to:**
- **Only** single-shard commits at `Durability::Buffered` or `Durability::None` with no condition (`check_and_mutate`), transaction or returned columns.
- **Never** cross-shard commits, the default `Durability::GroupSync` (which never engages it), flushes or compactions. Those stay on the shard's thread.
- **Engine-owned databases only:** with `open_application_owned`, your threads already run the shards.

**Turn it on for** buffered writers of large values or wide rows that care about put tail latency more than throughput. **Leave it off for** hot, short commits.

**Measured** on the same machine as above (16 threads, contended, `Buffered`; Pigeonhole without → with caller-runs):

| Workload | Without | With |
|---|--:|--:|
| Sparse wide rows, put p99.9 | 473 µs | 94 µs |
| YCSB E, put p99.9 | 1.8 ms | 0.24 ms |
| YCSB A with 16 KiB values, throughput | 97K/s | 110K/s (+13%) |
| Sparse wide-row deletes, throughput | 87K/s | 97K/s (+11%) |
| Counters, throughput | 1.70M/s | 1.40M/s (−18%) |
| YCSB A, throughput | 1.52M/s | 1.36M/s (−10%) |
| YCSB A, put p99 | 20 µs | 27 µs |
| Skewed load over many shards, against RocksDB | about level | 1.3–1.4× slower, p99 1.6–1.8× higher |

**The costs:**
- **Short commits lose throughput:** counters lost 18% and YCSB A and its variants 9–11%.
- **Put p99 rises 20–40%** on short-commit loads: the committing thread now does the shard's work itself rather than handing it off.
- **Skewed loads queue:** while one thread runs a hot shard, every other thread committing to that shard waits for it.

With default durability nothing changes, since caller-runs never engages.

The full evaluation is on [#1176](https://github.com/CodingAnarchy/pigeonhole/issues/1176) and [#1502](https://github.com/CodingAnarchy/pigeonhole/pull/1502). The decision is D321.
