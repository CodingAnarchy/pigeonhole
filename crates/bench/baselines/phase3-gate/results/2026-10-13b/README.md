# Phase 3 gate window, 2026-10-13b (reference machine)

The fourth Phase 3 gate run on the reference machine (#405), and the first that ran every step on a single commit. The hardware is the same as [2026-10-13](../2026-10-13/README.md): a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on a9ecfc77, the head of `main` after #642, from a fresh anonymous clone, with shard threads pinned (D235). That commit has D236 through D243, F1 (#642, D124), P1, #641 and #645. All 18 steps passed in one pass with nothing resumed, 109 minutes of steps in all. The box was quiet at the start (load 0.06) and no other agent used it during the window.

- **Start here:** `summary.md` has every #406 target with its value and verdict, and the decision tables.
- **New since 2026-10-13:**
  - warmup runs on the measured phase's client threads until placement settles (D242);
  - every `all` run measures for at least 10 s, and reproducibility is judged only on runs of 10 s or more (D243). Every row of this pair qualified, so `all-uring-compare.txt` has no "Not judged" list.
- **Verdicts: every judged target passes.**
  - Point get in memory: p50 0.78 µs, p99 5.79 µs.
  - Cold get: 1.028 device reads and 1.026 block-cache misses per get, p50 87 µs, p99 181 µs (64 GiB table, 1 GiB block cache, direct I/O, one thread).
  - Durable group commit: 9.55M cells/s at 16 shards, p50 84 µs, **p99 144 µs**.
  - Ordered scan from cache: 5.07 GB/s per core, against a target of 1 GB/s.
    - 2026-10-13 measured 6.59 GB/s, so this rate moves between runs.
  - Open to first read: median 2.6–3.1 ms.
  - Thread-per-core scaling within one CCD (D238): N = 1: 0.96, N = 4: 1.15, N = 8: 1.03.
  - Balancer placement at 4, 8 and 16 (D231).
  - Within 1.5× of RocksDB on every workload, in both runs.
    - `skewed-multi-shard`: RocksDB's throughput is 1.40× and 1.39× Pigeonhole's, and Pigeonhole's p99 is 1.28× and 1.30× RocksDB's. On 2026-10-13 the second pair was outside 1.5× (1.93× and 1.66×).
  - **Run-to-run reproducibility (D243): pass.** Every metric is within tolerance. Two rows did different background work in their measured phases, and the comparison notes them.
  - The Phase 2 floors (D193), both runs.
  - #387's ratios to SQLite EAV: row read 1.33× and 1.34×, scan 1.12× and 1.21×.
- **Accepted (D239):** memtable lookup at 2^18 entries, 337 ns.
- **Reported, not judged (D238):**
  - scaling at N = 16: 0.64 pinned and 0.58 unpinned;
  - scaling against the fixed 417,922 commits/s single shard: 0.95, 1.14, 0.98 and 0.64.
- **D244 lands after this commit.** D244 (#654, fixing #653) keeps a participant's cross-shard commit timestamps in seqno order. It changes the cross-shard commit path, adding a refusal and a retry, so the steps that measure that path will be rerun on the fixed commit before 0.3.0:
  - group commit and `latency`;
  - the cross-shard and skewed rows (`skewed-multi-shard` in `all` and in `latency`);
  - `scaling`.

  Until that rerun, these numbers describe a9ecfc77 without D244.
- **Not committed:** the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`). They are in the window's tarball, `results-20261013b.tar.gz`.
- **Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection. The measured fdatasync (p50 24 µs, p99 71 µs) agrees.
