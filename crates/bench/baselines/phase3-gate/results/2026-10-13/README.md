# Phase 3 gate window, 2026-10-13 (reference machine)

The third Phase 3 gate run on the reference machine (#405), on the same hardware as [2026-10-12](../2026-10-12/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on 8654ad69: D236, D237, D238 and D239 merged, shard threads pinned (D235). Every step except `cold-get` ran, 62 minutes of steps in all. The box was quiet at the start (load 0.00), and `gate-notes.txt` records the load at each start and end.

- **Start here:** `summary.md` has every #406 target with its value and verdict, and the decision tables.
- **New since 2026-10-12:**
  - scaling is judged per chiplet (D238), N = 1, 4 and 8 here, with the CCD size read from the L3 topology (`machine.json`: `ccd_cores` 8);
  - N = 16 and efficiency against a fixed single-shard rate are reported, not judged;
  - balancer placement is judged on its own (D231; `placement-{4,8,16}.json`);
  - the memtable lookup is judged at 2^18 entries (D230) and accepted at 337 ns (D239).
- **Verdicts:**
  - **Pass:**
    - point get in memory (p50 0.80 µs, p99 5.63 µs);
    - durable group commit: 9.75M cells/s, **p99 138 µs**, against 241 µs on 2026-10-12;
    - ordered scan from cache (6.59 GB/s);
    - open to first read (median 2.6–3.0 ms);
    - thread-per-core scaling within one CCD (N = 1: 0.90, N = 4: 1.10, N = 8: 0.99);
    - balancer placement at 4, 8 and 16;
    - within 1.5× of RocksDB on every workload;
    - the Phase 2 floors;
    - #387's ratios to SQLite EAV (row read 1.32× and 1.47×, scan 1.17× and 1.25×).
  - **Accepted (D239):** memtable lookup at 2^18 entries, 339 ns.
  - **Reported (D238):**
    - scaling at N = 16: 0.70 (pinned) and 0.59 (unpinned);
    - against the fixed 417,922 commits/s single shard: 0.90 / 1.10 / 1.00 / 0.69.
  - **Fail: run-to-run reproducibility** (`all-uring-compare.txt`), on one row only. That's `group-commit` with 16 synchronous client threads (not the judged D213 shape): p99 175 µs → 247 µs, +40.9% against a ±40% tolerance.
  - **Not run: cold get.** The first attempt's 64 GiB load stopped on `Busy` (a 30 s write stall). The diagnosis found the stall predates this window's changes:
    - the whole load stays on one shard and one tablet, never split;
    - compaction falls behind, with L0 past 1,000 SSTs.

    The investigation is ctr274's (splitting and the stall timeout) and perf2's (compaction under a deep L0). `cold-get` runs last, once that's understood, and its results are added to this directory.
- **Worth knowing:** scaling's N = 1 efficiency is 0.90, which means two single-shard runs in the same step measured 417K and 375K commits/s. The single-shard rate varies about that much between runs.
- **Not committed:** the raw step logs, criterion's HTML reports, the step markers (`*.done`, `*.status`) and the failed load's `cold-get.jsonl` (its machine line only).
- **Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection. The measured fdatasync (p50 25 µs, p99 77 µs) agrees.
