# Phase 2 gate baseline (D193, D208)

The Phase 2 gate's baseline is two official runs on the reference machine (D208). Under D193, every later official gate run (before each release) must not regress from it. The Mac runs 5 and 6, the baseline until then, stay here as the Phase 2 record.

## The reference baseline (D208)

| | |
|---|---|
| Files | `reference-run1.json`, `reference-run2.json` (runs 2 and 6 below) |
| Code | main `f8afbb2d` (D252–D254); owner decision 2026-10-14 (D208's History) |
| Machine | Hetzner AX102 (Ryzen 9 7950X3D, 128 GB ECC), Ubuntu 24.04, kernel 6.8, `/data` XFS on a Samsung PM9A3; the reference machine (#405) |
| Load | about 1.0, no other agent on the box |
| Command | `run-gate.sh`, from the checkout: `phdb-bench sparse-wide --engine pigeonhole,sqlite,rocksdb --scale full`, built with the `sqlite` and `rocksdb` features |

The JSON's `label` says `non-reference (D5)` because `PHDB_BENCH_REFERENCE` wasn't set; the gate window's own `phase2-gate` runs record it the same way.

**How the two were picked.** They're the 2 of 8 back-to-back official runs nearest the median: the least summed relative deviation over throughput, p99, p99.9, row-read and scan p99. All 8 runs (Pigeonhole, µs):

| run | ops/s | p99 | p99.9 | row-read p99 | scan p99 | against the Mac limits |
|---|--:|--:|--:|--:|--:|---|
| 1 | 27,314 | 412 | 524 | 440 | 500 | pass |
| **2** | 27,055 | 424 | 569 | 481 | 532 | pass |
| 3 | 25,846 | 434 | 618 | 510 | 569 | fail |
| 4 | 26,737 | 416 | 545 | 459 | 512 | pass |
| 5 | 25,862 | 436 | 610 | 483 | 557 | fail |
| **6** | 26,813 | 432 | 590 | 510 | 569 | fail |
| 7 | 28,550 | 444 | 618 | 524 | 573 | fail |
| 8 | 28,293 | 410 | 537 | 444 | 498 | pass |
| median | 26,934 | 428 | 580 | 482 | 545 | |

- **Get and put p99:** get p99 is 11–12 µs in every run, and put p99 is 17–38 µs.
- **#387's ratio to SQLite EAV on the two baseline runs:** row-read p99 is 1.35× and 1.45×, and scan p99 is 1.17× and 1.27×. Across the 8 runs it's 1.05–1.48× and 0.96–1.27×.

**Why the baseline is on f8afbb2d.**
- **The creep.** Sparse-wide's tail crept up over Phase 3: about 2%, spread across many engine commits with no single cause. A bisect over every engine commit since 8654ad69 on the AX102 showed this.
- **Against the Mac limits,** main failed `check.py` in 4 of the 8 runs above. Most of those failures were on scan p99.
- **The owner's decision** was to install the baseline from current main and defer the creep to the Phase 4 simplification pass (#724).
- **Allowances.** They're unchanged (below). Re-deriving them from the AX102's spread is a separate owner decision.

**The feature build reads higher.** On the same commit, the official form reads about 13 µs (3%) higher on Pigeonhole's p99, and about 20 µs higher on scan p99, than a plain `phdb-bench` build (no `sqlite` or `rocksdb` features). The plain build's 10 interleaved rounds on f8afbb2d:

| | p99 (µs) | scan p99 (µs) |
|---|---|---|
| median | 417 | 521 |
| range | 414–444 | 494–569 |

1 run in 10 was past the Mac limits. Compare official runs with official runs. The cause (code layout, or the allocator with RocksDB's runtime linked in) isn't measured yet (#724).

## The Mac runs (Phase 2 record, D193)

| | |
|---|---|
| Files | `gate2-run5.json`, `gate2-run6.json` |
| Code | main `cdfeb56` |
| Machine | Apple M5, 10 cores, 24 GiB, macOS 26.5.2, APFS (non-reference, D5) |
| Load | 1.8 (run 5), 1.4 (run 6); other agents paused |
| Command | `phdb-bench sparse-wide --engine pigeonhole,sqlite,rocksdb --scale full` (10 shards, 64 MiB memtable, 256 MiB cache, buffered, tablets on, one client thread) |

Pigeonhole in these runs:

| ops/s | p99 µs | p99.9 µs | get p99 | put p99 | row-read p99 | scan p99 |
|--:|--:|--:|--:|--:|--:|--:|
| 28.7K / 30.4K | 389 / 395 | 528 / 541 | 98 / 10 | 16 / 16 | 454 / 458 | 489 / 495 |

## Comparing a new run

```sh
crates/bench/baselines/phase2-gate/run-gate.sh [OUT.json]
```

The script builds `phdb-bench` with the comparison stores, runs the gate, prints `phdb-bench compare` against each baseline run (tolerance 0.20 for every store's throughput and p50, 0.40 for p99), and runs `check.py` on Pigeonhole's gated measures. It exits nonzero if one regressed beyond noise. Both use the reference runs; `check.py CANDIDATE.json gate2-run5.json gate2-run6.json` checks against the Mac runs instead. `check.py CANDIDATE.json` also works on its own, on an existing run.

## Noise allowances

`check.py` compares each measure with the worst of the two baseline runs. The allowances come from the spread between the Mac's official runs of the same code (D193), and apply unchanged to the reference runs: runs 3/4 (main `376ea21`), runs 5/6 (`cdfeb56`), and runs 7/10 (`cdfeb56` with an option that was off).

| measure | spread in same-code runs | allowance |
|---|---|--:|
| throughput | up to 15% (runs 7/10: 28.8K / 33.2K) | fails below 0.80 × the lower baseline |
| overall p99 | up to 2.5% within a pair, 6.7% across runs 5–10 | 1.12 × |
| p99.9 | up to 7.4% (runs 3/4) | 1.15 × |
| row-read p99 | up to 4.6% across runs 5–10 | 1.12 × |
| scan p99 | up to 8.4% (runs 3/4) | 1.12 × |
| get p99 | bimodal, 8–98 µs | 3 × (catches only a gross loss) |
| put p99 | 16–39 µs | 3 × |

Get and put p99 are dominated by rare stalls, so only a large regression is caught there. The other measures catch losses of roughly 10% or more.

A regression beyond these allowances blocks the gate or release until it's fixed or the owner accepts it (D193). Don't loosen an allowance to make a run pass; widen one only from new same-code evidence, with the owner's agreement.
