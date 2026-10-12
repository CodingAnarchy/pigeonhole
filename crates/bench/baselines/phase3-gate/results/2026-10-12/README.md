# Phase 3 gate window, 2026-10-12 (reference machine)

The second Phase 3 gate run on the reference machine (#405), on the same hardware as [2026-10-11](../2026-10-11/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP). It ran `run-window.sh` at full scale, 94 minutes in all, every step at a8b167e.

- **Start here:** `summary.md` has every #406 target with its value and verdict, and the decision tables.
- **New steps since 2026-10-11:**
  - durable group commit at 4 and 8 commits in flight per shard (D226; `latency-group-commit-inflight-{4,8}.json`);
  - the short group-commit run (D219; `latency-group-commit-short.json`);
  - the row cache off against its default (D216; `row-cache-off-*.json`);
  - the WAL pin bound (#527; `wal-pin.jsonl`), which decided D229: `wal_pin_bytes` stays 2× the memtable budget.
- **Verdicts:**
  - **Pass:** point get in memory; ordered scan from cache; open to first read; within 1.5× of RocksDB on every workload; the Phase 2 floors; #387's row-read and scan ratios to SQLite EAV, now within 1.5× (1.33–1.38× and 1.18–1.25×).
  - **Fail:** memtable lookup at 1M entries (#17, 484 ns); cold get, one I/O (1.106 reads per get); durable group commit p99 (241 µs against 200, D226); thread-per-core scaling at 8 and 16 shards (0.73 and 0.54; #154); run-to-run reproducibility (`all-uring-compare.txt`).
- **Not committed:** the raw step logs, criterion's HTML reports and the step markers (`*.done`, `*.status`). The full tarball is the `phase3-gate-2026-10-12` GitHub release asset.
- **Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection. The measured fdatasync (p50 25 µs, p99 75 µs) agrees.
