# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** a9ecfc773395ba26854f7d9325463696f7481dd6; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.78 µs, p99 5.79 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 337 ns (262k entries/miss (lower bound) 260 ns, 1M entries/hit (get) 483 ns, 1M entries/miss (lower bound) 403 ns, 10k entries/hit (get) 129 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.028 device reads, 1.026 cache misses per get; p50 87 µs, p99 181 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 9.55M cells/s at 16 shards (597K per core); p50 84 µs, p99 144 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 524 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 24 µs, p99 71 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 9.50M cells/s (-1% vs pinned); p50 83 µs, p99 166 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.71M cells/s (-51% vs 16); p50 48 µs, p99 80 µs, p99.9 121 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.20M cells/s (-25% vs 16); p50 61 µs, p99 94 µs, p99.9 162 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 10.53M cells/s at 16 shards (658K per core); p50 83 µs, p99 152 µs; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 131 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 5.03M cells/s at 16 shards; p50 161 µs, p99 561 µs; 0.2M commits in 0.2 s, 14 flushes, 0 compactions; in the engine p50 131 µs, p99 524 µs, p99.9 3146 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 70K cells/s, p99 118 µs, 4 thr: 232K cells/s, p99 151 µs, 16 thr: 596K cells/s, p99 189 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 5.07 GB/s (35029 misses, 381178 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.05 ms (p99 5.42), uring: 2.57 ms (p99 5.24) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 0.96; N=4: 1.15 (busiest 25%; 8.4 s, stalled 0% of shard time, 163 flushes, 43 compactions, max 10.6 ms); N=8: 1.03 (busiest 13%; 8.5 s, stalled 0% of shard time, 287 flushes, 76 compactions, max 59.1 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.64 (busiest 7%; 8.4 s, stalled 0% of shard time, 379 flushes, 91 compactions, max 48.3 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 0.95; N=4: 1.14; N=8: 0.98; N=16: 0.64 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.00; N=8: 0.90; N=16: 0.58 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 30.4% (max 31.2%), settled 15 s, 0 changes, 120K of 120K writes/s; N=8: busiest 14.8% (max 15.6%), settled 27 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.5% (max 7.8%), settled 21 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | all within | **pass** | `all-uring-1.json, all-uring-2.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 2 with different background work (D233) | **pass** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.33×, scan 1.12×; run 2: row read 1.34×, scan 1.21× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.56 | 0.51 | yes |
| all-uring-1 | group-commit | 0.59 | 1.19 | yes |
| all-uring-1 | skewed-multi-shard | 1.40 | 1.28 | yes |
| all-uring-1 | sparse-wide | 0.65 | 0.45 | yes |
| all-uring-1 | time-series-ttl | 1.01 | 0.89 | yes |
| all-uring-1 | ycsb-a | 0.58 | 0.48 | yes |
| all-uring-1 | ycsb-b | 0.32 | 0.61 | yes |
| all-uring-1 | ycsb-c | 0.40 | 1.08 | yes |
| all-uring-1 | ycsb-d | 0.44 | 0.93 | yes |
| all-uring-1 | ycsb-e | 0.80 | 0.91 | yes |
| all-uring-1 | ycsb-f | 0.72 | 0.62 | yes |
| all-uring-2 | adjacency | 0.60 | 0.53 | yes |
| all-uring-2 | group-commit | 0.57 | 0.81 | yes |
| all-uring-2 | skewed-multi-shard | 1.39 | 1.30 | yes |
| all-uring-2 | sparse-wide | 0.67 | 0.46 | yes |
| all-uring-2 | time-series-ttl | 0.98 | 0.80 | yes |
| all-uring-2 | ycsb-a | 0.61 | 0.51 | yes |
| all-uring-2 | ycsb-b | 0.32 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.41 | 1.10 | yes |
| all-uring-2 | ycsb-d | 0.48 | 0.94 | yes |
| all-uring-2 | ycsb-e | 0.82 | 0.97 | yes |
| all-uring-2 | ycsb-f | 0.70 | 0.57 | yes |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 373150 → 584277 | 2.40 → 0.78 µs | 4.7 → 5.8 µs | 74% |
| ycsb-a | 195265 → 179658 | 3.92 → 4.80 µs | 12.4 → 14.0 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 174192 | 4.8 | 14.1 | shard_parks 0.07, shard_wakes 0.07, commit_parks 0.00 |
| shards1 | 187368 | 4.4 | 13.6 | shard_parks 0.00, shard_wakes 0.01, commit_parks 0.00 |
| shards4 | 156472 | 4.6 | 15.0 | shard_parks 0.07, shard_wakes 0.07, commit_parks 0.00 |
| spin100 | 180198 | 4.7 | 14.0 | shard_parks 0.01, shard_wakes 0.01, commit_parks 0.00 |
| spin200 | 176140 | 4.6 | 14.3 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 190795 | 4.7 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 101329 | 6.1 | 28.8 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.04 | 0.97 |
| group-commit | 0.93 | 1.17 |
| skewed-multi-shard | 0.94 | 1.10 |
| sparse-wide | 1.05 | 0.96 |
| time-series-ttl | 0.94 | 1.05 |
| ycsb-a | 1.03 | 0.98 |
| ycsb-b | 1.01 | 1.00 |
| ycsb-c | 1.01 | 0.98 |
| ycsb-d | 1.07 | 1.00 |
| ycsb-e | 1.00 | 1.01 |
| ycsb-f | 0.99 | 0.99 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3534 |
| pread | 1 | 16 (D222) | 1035 |
| uring | 0 | 16 (D222) | 3165 |
| uring | 1 | 16 (D222) | 1022 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 396232 | 187 | 392 | 222 | 1216 MiB | 1216 MiB | 528 ms | 3065 µs |
| 4× budget | 377632 | 110 | 139 | 219 | 1472 MiB | 1472 MiB | 622 ms | 2110 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | a9ecfc773395 |
| build | 2 | a9ecfc773395 |
| memtable-lookup | 1 | a9ecfc773395 |
| phase2-gate | 3 | a9ecfc773395 |
| phase2-gate-2 | 3 | a9ecfc773395 |
| all-uring-1 | 15 | a9ecfc773395 |
| all-uring-2 | 15 | a9ecfc773395 |
| all-pread | 15 | a9ecfc773395 |
| latency | 5 | a9ecfc773395 |
| ycsb-a-shards | 3 | a9ecfc773395 |
| row-cache | 0 | a9ecfc773395 |
| scaling | 8 | a9ecfc773395 |
| placement | 1 | a9ecfc773395 |
| open-latency | 0 | a9ecfc773395 |
| wal-pin | 0 | a9ecfc773395 |
| cold-get | 36 | a9ecfc773395 |
| cold-scan | 2 | a9ecfc773395 |
| scan-cache | 0 | a9ecfc773395 |
| **total** | **109** | |
