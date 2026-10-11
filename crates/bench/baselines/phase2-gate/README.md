# Phase 2 gate baseline (D193, D208)

The official quiet runs 5 and 6 of the amended Phase 2 gate. Under D193 they're the baseline that every later official gate run (the Phase 3 gate, and before each release) must not regress from.

**Moving to the reference machine (D208).** The first gate window on the reference machine (#405) runs the gate twice (`phase2-gate`, `phase2-gate-2` in `../phase3-gate/run-window.sh`). Those two runs will become the baseline: commit them here as `reference-run1.json` and `reference-run2.json`. From then on `run-gate.sh` and `check.py` use them by default. These Mac runs stay as the Phase 2 record.

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

The script builds `phdb-bench` with the comparison stores, runs the gate, prints `phdb-bench compare` against each baseline run (tolerance 0.20 for every store's throughput and p50, 0.40 for p99), and runs `check.py` on Pigeonhole's gated measures. It exits nonzero if one regressed beyond noise. `check.py CANDIDATE.json` also works on its own, on an existing run.

## Noise allowances

`check.py` compares each measure with the worst of the two baseline runs. The allowances come from the spread between official runs of the same code: runs 3/4 (main `376ea21`), runs 5/6 (`cdfeb56`), and runs 7/10 (`cdfeb56` with an option that was off).

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
