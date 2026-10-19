# Phase 3 gate window, 2026-10-19 (reference machine, 0.3.0 candidate)

This is the 0.3.0 full gate on the reference machine (#405), rerun after [2026-10-18](../2026-10-18/README.md) failed its durable group commit target. The hardware is the same as [2026-10-14](../2026-10-14/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on 5933e23e from a fresh anonymous clone, with shard threads pinned (D235). Every step ran on that one commit, with no seeding from earlier windows.
- **The commit:** the head of `main` after #746. Since f61510dd (2026-10-18) it adds packing that sizes the active shards by the commits in flight (D257, #741), a warm-up that waits for a pending growth (#745) and the trim before every step (D258, #746).
- **Every step finished**, with none failed, timed out or skipped, in 317 minutes (`durations.tsv`).

- **Start here:** `summary.md`.

## Every step started on a trimmed drive (D258)
- `fstrim.tsv` lists each step, the time the trim finished, the bytes fstrim reported and its seconds.
- XFS reports every free byte it sent a discard for, not what the drive had mapped. That is why the count stays near the free space (1.5 TiB) before every step.
- The NVMe namespace was 289.7 of 1920.4 GB mapped (15.1%) at setup (`machine.json`).

## Durable group commit is back
| Window | Commit | cells/s | p50 | p99 | in the engine p50 / p99 |
|---|---|--:|--:|--:|---|
| 2026-10-14c | 234c5c2f | 9.54M | 87 µs | 143 µs | 66 / 131 µs |
| 2026-10-18 | f61510dd | 5.69M | 168 µs | 222 µs | 131 / 197 µs |
| 2026-10-19 | 5933e23e | 10.38M | 74 µs | 132 µs | 66 / 98 µs |
| 2026-10-19, pread | 5933e23e | 11.46M | 73 µs | 151 µs | 66 / 131 µs |

## Verdicts (`summary.md`)
- **Pass:**
  - point get in memory (p50 0.80 µs, p99 5.79 µs);
  - point get on cold data (1.05 device reads per get, p99 190 µs);
  - durable group commit (above);
  - ordered scan from cache (6.43 GB/s);
  - open to first read (median 2.92–3.42 ms);
  - scaling within one CCD (N = 1: 0.94, N = 4: 1.11, N = 8: 1.02);
  - balancer placement at 4, 8 and 16;
  - the Phase 2 floors (both runs);
  - #387's ratios to SQLite EAV (row read 1.32× and 0.94×, scan 1.17× and 1.03×);
  - the contended cross-shard row at the default durability (97K ops/s, RocksDB 1.12×, cross-shard p99 276 µs).
- **Accepted (D239):** memtable lookup at 2^18 entries, 337 ns.
- **Fail:**
  - **Reproducibility (D243):** one metric, ycsb-f's Pigeonhole p50 at one thread (10.2 against 12.7 µs, +25% where ±20% is allowed). It is tracked as #747 (Phase 4).
  - **Within 1.5× of RocksDB, and the D246 row in `all`:** cross-shard-contention in both runs (below). These passed on f61510dd.
  - **Contended at buffered:** ycsb-e (RocksDB 3.91×), cross-shard (RocksDB 1.72× throughput, p99 3.77×) and counters.
  - **Contended at the default durability:** ycsb-e (RocksDB 3.07×), ycsb-f (SQLite p99 2.13×), cross-shard (SQLite p99 2.45×) and counters.
- **Reported rows past the 3× rule:** transfer at both levels, application-owned mode at buffered (0.19× the scaling gate's N = 16) and time-series-range at buffered. ycsb-a through the async API at 16 in flight, buffered, ran at 0.06× the synchronous run.

## The owner's pass condition is not met
The owner decided that the gate passes if the D243 ycsb-f miss (#747) is its only failure beyond those already accepted for Phase 4. Those are contended ycsb-e, counters, transfer and the cross-shard tails against SQLite. The D246 cross-shard row in `all` is a further failure, so that condition does not hold for this window.

## Packing slows cross-shard-contention in `all`
cross-shard-contention in `all`: Pigeonhole's throughput, p99 and p99.9 by configuration.

| Run | tablets on (packing on, the default) | tablets off | tablets on, packing off | RocksDB |
|---|---|---|---|---|
| 2026-10-18 uring 1 | 477.9K, 133 / 477 µs | 461.9K | 490.4K | 569.5K |
| 2026-10-19 uring 1 | 398.5K, 170 / 4424 µs | 586.9K | 590.1K | 579.3K |
| 2026-10-19 uring 2 | 398.5K, 167 / 3113 µs | 580.7K | 573.5K | 578.0K |
| 2026-10-19 pread | 394.4K, 170 / 3867 µs | 605.5K | 610.5K | 570.2K |

- Within each run, the default configuration is about a third slower than packing off, and its p99.9 is 3–4 ms. On f61510dd the two were within 3%.
- The refusals record tablets moved (45 and 26), where f61510dd's runs moved none.
- Of the three changes since f61510dd, only D257 and #745 change the engine. This has not been bisected.

## Not committed
Same as earlier windows: the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`).

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection.
