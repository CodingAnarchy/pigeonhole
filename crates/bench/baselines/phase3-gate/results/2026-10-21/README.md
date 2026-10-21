# Phase 3 gate window, 2026-10-21 (reference machine, main after #778)

This is gate 21. It is a full gate on the reference machine (#405), run to cover #777 (#778) and #724 area 1 on top of [2026-10-20](../2026-10-20/README.md) (gate 20, 0.3.0). The hardware is the same: a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on 3ee90c21, in a fresh data directory, with shard threads pinned (D235). Every step ran on that one commit.
- **The commit:** the head of `main` after #778. Since 516b59b8 (gate 20) it adds:
  - a one-round cross-shard commit, with its seqno and timestamp picked at the decision (D263, #774; format version 3);
  - the cleanups D263 made possible: the retry presets, `BelowFloor`, one `ts_raises` and one `RetryWait` (#779, #782, #784, #785; D264);
  - the release of commits waiting behind a retry that lands on one shard (#772);
  - the test-hook registries (#789, #790) and the writer-lock handover (D265, #788);
  - flushed shares indexed by slot (#777, #778).
- **Every step finished**, with none failed, timed out or skipped, in 321 minutes (`durations.tsv`).
- **The owner's verdict is pending.** The verdicts below are the summary's.
- **Start here:** `summary.md`.

## Every step started on a trimmed drive (D258)
- `fstrim.tsv` lists each step, the time the trim finished, the bytes fstrim reported and its seconds.
- The NVMe namespace was 310.0 of 1920.4 GB mapped (16.1%) at setup (`machine.json`).

## Verdicts (`summary.md`)
- **Pass:**
  - point get in memory (p50 0.83 µs, p99 5.70 µs);
  - point get on cold data (1.05 device reads per get, p99 192 µs);
  - durable group commit (10.50M cells/s, p50 73 µs, p99 130 µs; in the engine p50 66 µs, p99 98 µs);
  - ordered scan from cache (4.26 GB/s; see below);
  - open to first read (median 2.94–3.34 ms);
  - scaling within one CCD (N = 1: 1.00, N = 4: 1.11, N = 8: 1.01);
  - balancer placement at 4, 8 and 16;
  - the Phase 2 floors (both runs);
  - #387's ratios to SQLite EAV (row read 1.07× and 1.40×, scan 1.00× and 1.21×);
  - within 1.5× of RocksDB on every `all` workload;
  - **reproducibility (D243): the runs agree within tolerance** (gate 20 missed three metrics, #768);
  - the contended cross-shard row at the default durability (131K ops/s, RocksDB 0.83×, cross-shard p99 266 µs; gate 20: 107K, 1.03×).
- **Accepted (D239):** memtable lookup at 2^18 entries, 335 ns.
- **FAIL, new: the D246 cross-shard row in `all`, by one check in run 1.** Run 1's cross-shard commit p99 is 162 µs, **1.52×** RocksDB's batch p99 (limit 1.5×). Run 2 reads 158 µs (1.47×). Every other check on the row passes in both runs (below).
- **FAIL, the same rows as gate 20:**
  - **Contended at buffered:** ycsb-e (RocksDB 3.67×), cross-shard (RocksDB 1.19× throughput, p99 3.02×; gate 20: 1.34× and 2.95×) and counters.
  - **Contended at the default durability:** ycsb-e (RocksDB 2.84×), ycsb-f (SQLite p99 1.86×, #755), cross-shard (SQLite p99 2.28×) and counters.
- **Reported rows past the 3× rule:** transfer at both levels, application-owned mode at buffered (**0.07×** the scaling gate's N = 16; gate 20: 0.20×, below) and ycsb-a through the async API at 16 in flight, buffered (0.25× the synchronous run; gate 20: 0.41×).

## The cross-shard row in `all`: faster, with a higher p99
cross-shard-contention in `all`: Pigeonhole's throughput and overall p99 / p99.9 (µs) by configuration.

| Run | tablets on (packing on, the default) | tablets off | tablets on, packing off | RocksDB |
|---|---|---|---|---|
| 2026-10-20 uring 1 | 593.1K, 134 / 411 | 590.0K, 137 / 346 | 596.3K, 133 / 475 | 571.5K, 100 / 442 |
| 2026-10-20 uring 2 | 583.8K, 134 / 370 | 591.7K, 136 / 389 | 589.2K, 133 / 442 | 567.5K, 104 / 315 |
| 2026-10-21 uring 1 | 645.7K, 147 / 456 | 640.3K, 145 / 520 | 645.1K, 146 / 593 | 570.8K, 103 / 325 |
| 2026-10-21 uring 2 | 660.8K, 143 / 626 | 650.3K, 143 / 440 | 642.7K, 143 / 518 | 571.5K, 104 / 446 |
| 2026-10-21 pread | 666.6K, 147 / 561 | 654.1K, 147 / 522 | 660.8K, 143 / 659 | 570.8K, 102 / 337 |

- **Throughput and p99:** throughput is up 9–11% on gate 20 in every configuration (RocksDB 0.86–0.88×), and the p99 is up about 8%. That matches #774's A/B for one-round commits (+7–18% with a higher tail), which moves the cross-shard commit p99 from 137 to 158–162 µs against RocksDB's 103–108 µs batch p99.
- **Packing and tablets:** tablets on/off is 1.01 and 0.99 (packing off).
- **Refusals:** none for a moved tablet, below a floor or for room. 227–262 pending-overlap refusals (D249), and wasted rounds are 0.000 per cross-shard commit (gate 20: 0.012–0.013, all below-floor, a path D263 removed).

## Application-owned mode at buffered collapses on overlap refusals
`contended-buffered-app-owned.json`, 16 clients inline, about 20% cross-shard batches:

| | gate 20 (516b59b8) | gate 21 (3ee90c21) |
|---|--:|--:|
| commits/s | 886K | 322K |
| p50 / p99 | 168 µs / 512 µs | 47 µs / 20.6 ms |
| two-phase rounds | 7.96M | 18.63M |
| refused rounds | 146K (below floor) | 15.50M |
| pending-overlap refusals (D249) | 0 | 17.82M |
| share waits | — | 3.64M, 59.4 s in total |
| write stalls | 1850 (6.1 s) | 3 (73 ms) |

Five of every six rounds are refused for an overlap with a pending share, and the share waits sum to about 59 s. The default durability row shows the same pattern: 120K commits/s and p99 48 ms against gate 20's 393K and 4.0 ms, with 9.17M of 10.35M rounds refused (gate 20: 48K of 3.96M). It is not root-caused yet. The likely cause: under D263 a pending share now covers the window from the decision until the share is applied everywhere, and with application-owned shards driven inline, a refused round's retry meets the same pending share again. This needs an issue (the summary flags it).

## Other changes against gate 20
- **Ordered scan from cache, 6.75 → 4.26 GB/s:** a sizing effect in the harness, not a slower scan. `coldscan warm` sizes the block cache at 1.25× the file plus 64 MiB. The file was smaller this time (cache 3904 MiB against 4624 MiB), so the decoded blocks no longer fit: 243,738 misses against 30,272 hits, where gate 20 had 0 misses. It still passes the 1 GB/s target. Sizing the cache from the table's decoded size would keep this row measuring the cache.
- **ycsb-a through the async API at 16 in flight, buffered:** 297K ops/s, 0.25× the synchronous run (gate 20: 473K, 0.41×; gate 19: 0.06×). It has no cross-shard commits, and its write stalls doubled (6,750 → 13,207). This row has swung on every gate (#749).
- **Contended cross-shard at buffered:** 475K → 537K ops/s, RocksDB 1.34× → 1.19×, with its p99 ratio about level (2.95× → 3.02×).

## Not committed
Same as earlier windows: the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`).

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection.
