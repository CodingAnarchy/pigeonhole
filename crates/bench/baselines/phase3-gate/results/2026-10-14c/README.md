# Phase 3 gate window, 2026-10-14c (reference machine, 0.3.0 candidate): FAILED, stopped early

This is the 0.3.0 full gate on the reference machine (#405). The hardware is the same as [2026-10-14](../2026-10-14/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on 234c5c2f from a fresh anonymous clone, with shard threads pinned (D235).
- **The commit:** the head of `main` after #726. It adds D251–D254, the D246 amendments (#720) and the AX102 Phase 2 baselines (D208, #726) to 2026-10-14's 175c0ffc.
- **The window did not finish.** `contended` failed, a commit returning Busy (below). The window was resumed past it, then stopped by the owner during cold-get. The commit is being replaced by a fix for that failure.

- **Start here:** `summary.md`. report.py was run alone on what did run.
- **Records of the two interrupted steps:** `contended.failed` and `cold-get.aborted`.

## What ran and what didn't
- **Ran:** setup, build, memtable-lookup, phase2-gate ×2, all-uring ×2, all-pread, latency, ycsb-a-shards, row-cache, scaling and placement, all in one pass. Then open-latency and wal-pin, after the resume.
- **`contended`: FAILED, marked done by hand** so the remaining steps could run.
  - 39 of its 40 runs finished, about 200 min.
  - The default-level ycsb-a run with riders returned `Busy: stalled past the write-stall timeout (30 s)`, so `contended-default-ycsb-a-riders.*` is missing.
  - Its buffered run finished but collapsed: 8.1K ops/s against 712.5K on 175c0ffc, with 0 flushes, 1,780 write stalls (44.7 s), put p99 98 ms, and every commit on shard 0.
- **`cold-get`: ABORTED** (owner's call). Its 64 GiB load under D252 had written 2.9 TB in 62 min (about 750 MB/s, all compaction churn) and would not have finished within its limit.
- **Not run:** `cold-scan`, `scan-cache`. The cold get and cached scan targets show as "not run" in `summary.md`.

## Why the riders row failed (measured after the window, not committed here)
These were short runs of the riders row (buffered), 2 rounds per commit.
- **The cause is D252** (#721, flush and compaction on dedicated threads by default):

  | commit | ops/s | flushes |
  |---|--:|--:|
  | 737bc13b (before D251) | 617–704K | 199–202 |
  | bcea95e2 (D251 and D253) | 670–754K | 135–164 |
  | a46c7d5c (D252) | 5.4K, then a hang | 6 |
  | 234c5c2f | 3.0K, then a hang | 8 |

- **The I/O backend matters.** With `PIGEONHOLE_IO=pread` the same commit is healthy (547K ops/s). Turning `pin_threads` off doesn't help.
- **Stacks taken during the stall** show the flush and compaction threads idle with no work queued, and no io_uring ring backed up: a flush that is never queued, not an I/O hang.
- The fix and its confirmation are tracked separately. The next gate reruns `contended`, `cold-get`, `cold-scan` and `scan-cache` on the fixed commit.

## Verdicts (`summary.md`)
- **Pass:**
  - point get in memory (p50 0.80 µs, p99 5.73 µs);
  - durable group commit (9.54M cells/s, p99 143 µs);
  - open to first read (median 2.9–3.4 ms);
  - scaling within one CCD (N = 1: 1.05, N = 4: 1.09, N = 8: 0.98);
  - balancer placement at 4, 8 and 16;
  - the Phase 2 floors against the new AX102 baselines (both runs);
  - #387's ratios to SQLite EAV (row read 1.28× and 1.26×, scan 1.11× and 1.05×);
  - the contended cross-shard row at the default durability (98K ops/s, RocksDB 1.10×, cross-shard p99 260 µs, under the 1 ms cap).
- **Accepted (D239):** memtable lookup at 2^18 entries, 339 ns.
- **Fail:**
  - **Within 1.5× of RocksDB:** only `cross-shard-contention` is outside it, at RocksDB 2.57× and 2.34× throughput (3.09× and 3.06× on 175c0ffc).
  - **The D246 row in `all`:**
    - fails on the RocksDB ratio and the batch-p99 ratio (2.16× and 2.06×);
    - passes on tablets on/off with packing off (0.99, 0.98), wasted rounds (0.008), Busy (0) and the 2 ms cap (cross-shard p99 223 and 211 µs).
  - **Contended at buffered:** ycsb-e, ycsb-f, cross-shard and counters.
  - **Contended at the default durability:** ycsb-e, ycsb-f and counters.
  - **Reproducibility (D243):** the unpinned group-commit rider differs between the two runs (2.34M against 1.70M, p50 85 against 151 µs). The sparse-wide and time-series-ttl rows also had different background work.
- **Not run:** cold get, cached scan.
- **Reported rows past the 3× rule:**
  - transfer at both levels;
  - the async API at 16 in flight at both levels, and at 4 in flight at buffered;
  - application-owned mode at buffered;
  - time-series-range at buffered;
  - ycsb-a with riders at buffered (the failure above).

## Durations
`durations.tsv` lists 78 minutes of finished steps. It leaves out `contended` (about 200 min before it failed, marked done by hand) and cold-get's 62 aborted minutes.

## Not committed
Same as earlier windows: the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`), plus cold-get's partial `cold-get.jsonl`, which holds only the machine record. The riders measurements and stacks are kept with the coordinator.

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection.
