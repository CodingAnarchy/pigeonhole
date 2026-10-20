# Phase 3 gate window, 2026-10-20 (reference machine, 0.3.0)

This is gate 20, the 0.3.0 full gate on the reference machine (#405), rerun after [2026-10-19](../2026-10-19/README.md) failed the D246 cross-shard row in `all`. The hardware is the same: a Hetzner AX102 (Ryzen 9 7950X3D, 2 CCDs × 8 cores, 32 threads, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3 (PLP).

It ran `run-window.sh` at full scale on 516b59b8 from a fresh anonymous clone, with shard threads pinned (D235). Every step ran on that one commit, with no seeding from earlier windows.
- **The commit:** the head of `main` after #764. Since 5933e23e (2026-10-19) it adds:
  - D259's latency floor in the reproducibility check (#752);
  - D261's per-operation p99 table, reported beside the judged mixed p99 (#756);
  - the conditional-commit wait (D260, #750) and the warm-up's tablet-count wait (#751);
  - a 200 µs shard spin (#753);
  - flushed-share reports only on real progress (#761);
  - a cross-shard commit's time in flight counted at every shard its round occupies (D262, #762);
  - the WAL checkpoint popping at most 64 records per batch (#764).
- **Every step finished**, with none failed, timed out or skipped, in 320 minutes (`durations.tsv`).
- **The owner's verdict: the gate passes.** The reproducibility miss is accepted as a Phase 4 issue (#768), as ycsb-f's was last time (#747).

- **Start here:** `summary.md`.

## Every step started on a trimmed drive (D258)
- `fstrim.tsv` lists each step, the time the trim finished, the bytes fstrim reported and its seconds.
- The NVMe namespace was 287.1 of 1920.4 GB mapped (15.0%) at setup (`machine.json`).

## Verdicts (`summary.md`)
- **Pass:**
  - point get in memory (p50 0.80 µs, p99 5.98 µs);
  - point get on cold data (1.05 device reads per get, p99 192 µs);
  - durable group commit (10.07M cells/s, p50 74 µs, p99 133 µs; in the engine p50 66 µs, p99 98 µs);
  - ordered scan from cache (6.75 GB/s);
  - open to first read (median 2.94–3.41 ms);
  - scaling within one CCD (N = 1: 1.00, N = 4: 1.16, N = 8: 1.02);
  - balancer placement at 4, 8 and 16;
  - the Phase 2 floors (both runs);
  - #387's ratios to SQLite EAV (row read 1.28× and 1.34×, scan 1.12× and 1.22×);
  - **within 1.5× of RocksDB on every `all` workload, and the D246 cross-shard row in `all`** (below);
  - the contended cross-shard row at the default durability (107K ops/s, RocksDB 1.03×, cross-shard p99 255 µs).
- **Accepted (D239):** memtable lookup at 2^18 entries, 338 ns.
- **Accepted for Phase 4:**
  - **Reproducibility (D243):** three metrics (below), #768.
  - **Contended at buffered:** ycsb-e (RocksDB 3.90×), cross-shard (RocksDB 1.34× throughput, p99 2.95×; gate 19: 1.72× and 3.77×) and counters.
  - **Contended at the default durability:** ycsb-e (RocksDB 3.27×), ycsb-f (SQLite p99 2.04×, #755), cross-shard (SQLite p99 2.29×) and counters.
- **Reported rows past the 3× rule:** transfer at both levels, application-owned mode at buffered (0.20× the scaling gate's N = 16) and time-series-range at buffered.
- **ycsb-a through the async API at 16 in flight, buffered:** 0.41× the synchronous run (473K ops/s). It was 0.06× (71K) on gate 19 (#749).

## The cross-shard row in `all` is back
cross-shard-contention in `all`: Pigeonhole's throughput, p99 and p99.9 by configuration.

| Run | tablets on (packing on, the default) | tablets off | tablets on, packing off | RocksDB |
|---|---|---|---|---|
| 2026-10-19 uring 1 | 398.5K, 170 / 4424 µs | 586.9K | 590.1K | 579.3K |
| 2026-10-20 uring 1 | 593.1K, 134 / 411 µs | 590.0K | 596.3K | 571.5K |
| 2026-10-20 uring 2 | 583.8K, 134 / 370 µs | 591.7K | 589.2K | 567.5K |
| 2026-10-20 pread | 600.9K, 134 / 520 µs | 610.4K | 610.1K | 560.7K |

- **Packing:** the default configuration runs within 1–2% of packing off in every run (tablets on/off 1.01 and 1.00). Its p99.9 is 0.37–0.52 ms, against 3–4 ms on gate 19.
- **Refusals:** none records a moved tablet (gate 19: 45 and 26). Wasted rounds were 0.013 and 0.012 per cross-shard commit.

## Reproducibility misses one Pigeonhole row (#768)
`all-uring-compare.txt` fails three metrics (status 1):

| Row | Metric | Run 1 | Run 2 | Change | Allowed |
|---|---|--:|--:|--:|---|
| group-commit Pigeonhole, 16 threads, group-sync | throughput | 287.4K | 226.7K | −21.1% | ±20% |
| group-commit Pigeonhole, 16 threads, group-sync | p99 | 109 µs | 162 µs | +49.1% | ±40% or ±6 µs |
| adjacency RocksDB, 1 thread | throughput | 15.0K | 12.0K | −20.1% | ±20% |

**Group commit, run 1 is the outlier.** All three runs:

| Run | ops/s | p99 | p99.9 | visibility waits |
|---|--:|--:|--:|--:|
| uring 1 | 287K | 109 µs | 156 µs | 5.6K |
| uring 2 | 227K | 162 µs | 465 µs | 49.8K |
| pread | 206K | 173 µs | 414 µs | 55.6K |
| gate 19 uring 1 | 228K | 159 µs | | |
| gate 19 uring 2 | 206K | 180 µs | | |

Run 2 and pread sit at the usual level, and run 1 logged about 9× fewer visibility waits. The same workload at 1 and 4 threads reproduces within a few percent.

**Adjacency is RocksDB's own variance.** Its scan p99 was 590 µs in run 1, 795 µs in run 2 and 618 µs on pread.

**The load at each start doesn't explain either miss.** Run 1 started at a load of 1.03, run 2 at 13.60 and pread at 14.45. The higher readings are left over from the previous run's last sections, the D213 group-commit and scaling runs. That load decays within the first workload, long before either row runs.

## Not committed
Same as earlier windows: the raw step logs, criterion's HTML reports, `gate.log` and the step markers (`*.done`, `*.status`).

**Expected warning:** the PM9A3 reports a volatile write cache (`vwc 0x6`) although it has power-loss protection.
