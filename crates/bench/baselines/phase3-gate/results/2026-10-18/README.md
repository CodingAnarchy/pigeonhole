# Phase 3 gate window, 2026-10-18 (reference machine, 0.3.0 candidate)

This is the 0.3.0 full gate on the reference machine (#405), rerun after [2026-10-14c](../2026-10-14c/README.md) failed. The hardware is the same as [2026-10-14](../2026-10-14/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on f61510dd from a fresh anonymous clone, with shard threads pinned (D235). Every step ran on that one commit, with no seeding from earlier windows.
- **The commit:** the head of `main` after #737. It adds three fixes for 2026-10-14c's failure to 234c5c2f: compaction pool threads with io_uring rings of their own (#729), packing that counts write-stall time as busy (D255, #730), and packing that counts a shard thread's spin and the pool's work for it (D256, #737).
- **Every step finished**, with none failed, timed out or skipped. The first `setup` attempt refused to start, because load from a pre-window smoke run was still too high. The window was resumed once the machine was quiet, so no step was measured under that load.

- **Start here:** `summary.md`.

## 2026-10-14c's failure is gone
- **`contended` ran all of its runs, at both durability levels**, including ycsb-a with riders, which returned Busy on 234c5c2f.
  - At buffered, the riders' foreground p99 is 82 µs (1.79× the judged ycsb-a's). On 234c5c2f this row collapsed to 8.1K ops/s.
  - At the default durability, the riders' foreground p99 is 123 µs (1.14×), with no Busy.
- **`cold-get` finished in 37 min**, where 2026-10-14c's was aborted after 62 min of compaction churn. `cold-scan` and `scan-cache` ran too.

## Verdicts (`summary.md`)
- **Pass:**
  - point get in memory (p50 0.83 µs, p99 5.79 µs);
  - point get on cold data (1.05 device reads per get, p99 194 µs);
  - ordered scan from cache (5.11 GB/s);
  - open to first read (median 3.15–3.42 ms);
  - scaling within one CCD (N = 1: 0.99, N = 4: 1.12, N = 8: 0.94);
  - balancer placement at 4, 8 and 16;
  - the Phase 2 floors (both runs);
  - #387's ratios to SQLite EAV (row read 1.39× and 1.37×, scan 1.20× in both);
  - the D246 row in `all` (RocksDB 1.19× in both runs, cross-shard p99 137 and 138 µs). It failed on 175c0ffc and 234c5c2f.
  - the contended cross-shard row at the default durability (103K ops/s, RocksDB 1.03×, cross-shard p99 254 µs).
- **Accepted (D239):** memtable lookup at 2^18 entries, 339 ns.
- **Fail:**
  - **Durable group commit: p99 222 µs, over the 200 µs target.** This target passed in every window from 2026-10-13 to 2026-10-14c (below).
  - **Within 1.5× of RocksDB:** only ycsb-d in run 2 (p99 2.10×). Run 1 measured 0.98×, and Pigeonhole's ycsb-d p99 differs between the runs (14.7 against 31.7 µs).
  - **Contended at buffered:** ycsb-e (RocksDB 4.07×), cross-shard (1.37× throughput, but p99 2.72×) and counters (4.84×).
  - **Contended at the default durability:** ycsb-e (3.55×), ycsb-f (SQLite p99 1.92×), cross-shard (SQLite p99 2.24×) and counters.
  - **Reproducibility (D243):** 9 metrics differ beyond tolerance between the two runs, 5 of them RocksDB's. Pigeonhole's ycsb-a and sparse-wide rows had different background work.
- **Reported rows past the 3× rule:** transfer at both levels, application-owned mode at buffered (0.21× the scaling gate's N = 16), and time-series-range at buffered.

## Durable group commit fell between 234c5c2f and f61510dd
| Window | Commit | cells/s | p50 | p99 | in the engine p50 / p99 |
|---|---|--:|--:|--:|---|
| 2026-10-14c | 234c5c2f | 9.54M | 87 µs | 143 µs | 66 / 131 µs |
| 2026-10-18 | f61510dd | 5.69M | 168 µs | 222 µs | 131 / 197 µs |
| 2026-10-18, pread | f61510dd | 5.77M | 168 µs | 221 µs | 131 / 197 µs |

- The pread run fell by the same amount, so the I/O backend is not the cause.
- The engine's own commit latency doubled.
- Only #729, #730 and #737 are between the two commits. The drop has not been bisected.

## Durations
`durations.tsv` lists 318 minutes of steps.

## Not committed
Same as earlier windows: the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`).

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection.
