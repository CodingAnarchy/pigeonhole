# Phase 3 gate window, 2026-10-14 (reference machine, pre-release)

This is the pre-release full gate on the reference machine (#405). The hardware is the same as [2026-10-13b](../2026-10-13b/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran every step of `run-window.sh` at full scale on 175c0ffc, the head of `main` after #689, from a fresh anonymous clone, with shard threads pinned (D235).
- **On the commit:** D244, D245, D248, D249, #673, #688, and the `contended` step (D246, D247) with its time bound (#689) and D249's refusal reason (#701).
- **The run:** all 19 steps finished in one pass with EXIT 0, 314 minutes of steps. `contended` took 197 of them, against an estimate of about 160. The box was quiet at the start (load 0.00) and no other agent used it.

- **Start here:** `summary.md` has every #406 target with its value and verdict, the RocksDB table, the contended step's side-by-side table, and the decision tables.
- **New since 2026-10-13b:**
  - `cross-shard-contention` in `all`, with its D246 row (#659, #666, #682, #701);
  - the `contended` step (D247): 16 client threads on the default options, judged at D108 parity (buffered) and at the default durability (GroupSync), against RocksDB and SQLite;
  - the reported rows: transfer, sparse-wide deletes, filtered scans, 16 KiB values, the async API at 4 and 16 in flight, application-owned mode on the defaults, and ycsb-a's riders;
  - every contended run bounded at 3× its minimum (#689).

## Verdicts
- **Pass:**
  - point get in memory (p50 0.84 µs, p99 6.05 µs);
  - cold get, one I/O (1.059 device reads per get, p99 236 µs);
  - durable group commit (9.58M cells/s, p99 138 µs);
  - ordered scan from cache (6.49 GB/s);
  - open to first read (median 2.3–3.0 ms);
  - scaling within one CCD (N = 1: 1.01, N = 4: 1.12, N = 8: 1.00);
  - balancer placement at 4, 8 and 16;
  - reproducibility (D243);
  - #387's ratios to SQLite EAV (row read 1.36× and 1.16×, scan 1.35× and 1.32×);
  - the contended cross-shard row at the default durability (86K ops/s, RocksDB 1.25×, p99 1.40×, cross-shard p99 303 µs, 1.42× RocksDB's batch p99; the GroupSync cap is still to be set).
- **Accepted (D239):** memtable lookup at 2^18 entries, 339 ns.
- **Fail, new judged targets:**
  - **Within 1.5× of RocksDB on every workload:** only `cross-shard-contention` is outside it, in both runs: RocksDB 3.09× and 3.06× throughput, p99 10.6× and 10.8×. Every other workload is within.
  - **The D246 row in `all`:**
    - fails on the RocksDB ratio and tablets on/off 0.48;
    - passes on wasted rounds (0.004 per cross-shard commit), Busy (0) and the 2 ms cap (cross-shard p99 1.1 ms), but that p99 is 10.4× RocksDB's batch p99.
    - The 36× slowdown that started D246 is gone: 184K ops/s against about 11K on a9ecfc77.
  - **Contended at D108 parity (buffered), outside 1.5×:**
    - ycsb-a p99 (6.1× RocksDB);
    - ycsb-e (RocksDB 3.84×);
    - ycsb-f p99 (2.1× RocksDB);
    - cross-shard (RocksDB 2.98×, p99 15×; tablets on/off 0.71);
    - counters (RocksDB 3.58×; p99 84× SQLite's).
  - **Contended at the default durability (GroupSync), outside 1.5×:**
    - ycsb-e (RocksDB 3.84×);
    - ycsb-f p99 (2.2× SQLite's);
    - cross-shard p99 (2.7× SQLite's);
    - counters p99 (5.7× RocksDB's, 43× SQLite's).
- **Phase 2 floors (D193): FAIL, but not on Pigeonhole.**
  - The step compares against the Phase 2 baselines measured on the M5 (`gate2-run5.json`, `gate2-run6.json`).
  - The failing measures are SQLite EAV's and RocksDB's sparse-wide p50 (and SQLite's p99 in run 2) on the AX102 against those M5 numbers.
  - Pigeonhole's rows aren't compared at all: the settings key differs (10 shards on the M5, 16 here).
  - D208 meant the first window's runs to become the AX102 reference baseline. That needs the owner's call and isn't changed here.
- **Reported rows past the 3× rule (each "file an issue"):**
  - transfer at both levels (RocksDB 38× and 7.3× ahead; 1.22 and 1.97 conflicts per transfer, with 0.49 and 0.95 pending-share overlaps the engine now retries, D249);
  - ycsb-a through the async API at 16 in flight at buffered (0.13× the synchronous run, p99 17 ms);
  - application-owned mode on the defaults at buffered (741K commits/s, 0.18× the scaling gate's N = 16; no Busy, 2% of 6.8M two-phase rounds refused);
  - time-series-range at buffered (RocksDB 1.36× against time-series-ttl's 0.34×).
- **Capped runs (#689):** 12 contended runs stopped at their bound, among them Pigeonhole's default-level sparse-wide (176 s) and the time-series-range and adjacency-prefix runs. `summary.md` lists each.

## Not committed
The raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`). They're in the window's tarball, `results-20261014.tar.gz`.

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection. The measured fdatasync (p50 24 µs, p99 72 µs) agrees.
