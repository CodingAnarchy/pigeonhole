# Performance compared with SQLite

<!-- GATE 4 (release PR): refresh this page's figures from gate 4's #759 runs (equal cache memory, YCSB A and F, SQLite / Pigeonhole max / paced). -->

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

The benchmark is `phdb-bench` in this repository (`--cache-model total --cache 1073741824`, plus `--offered-rate` for the paced runs; [`docs/bench.md`](../bench.md) has the options). The full results are on [#759](https://github.com/CodingAnarchy/pigeonhole/issues/759); these numbers are its 2026-10-29 re-run on `cba09184`.
