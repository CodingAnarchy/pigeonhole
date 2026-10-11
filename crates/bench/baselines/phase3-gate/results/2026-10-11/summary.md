# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** f333d4879d6de2df994c326d4181eda9be3c3984; **scale:** full
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 3.38 µs, p99 12.67 µs | **FAIL** | `latency-ycsb-c.json` |
| Memtable point lookup at 1M entries: ≤ 300 ns (#17) | 1M hit 481 ns (1M entries/miss (lower bound) 402 ns, 10k entries/hit (get) 129 ns, 10k entries/miss (lower bound) 105 ns) | **FAIL** | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 2.092 device reads, 2.091 cache misses per get; p50 172 µs, p99 326 µs | **FAIL** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213) | 2.39M cells/s at 32 shards (75K per core); p50 291 µs, p99 1827 µs; busiest shard 3%; in the engine p50 131 µs, p99 524 µs, p99.9 2097 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 25 µs, p99 75 µs | **FAIL** | `latency-group-commit.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 2.90M cells/s at 32 shards (91K per core); p50 157 µs, p99 303 µs; in the engine p50 131 µs, p99 197 µs, p99.9 262 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 71K cells/s, p99 123 µs, 4 thr: 205K cells/s, p99 137 µs, 16 thr: 457K cells/s, p99 208 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.24 GB/s (0 misses, 271775 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.08 ms (p99 4.06), uring: 2.69 ms (p99 3.60) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N | N=4: 1.03 (busiest 32%), N=8: 0.54 (busiest 16%), N=16: 0.24 (busiest 15%) | **FAIL** | `scaling-N.json` |
| Within 1.5× of RocksDB on every workload | outside: skewed-multi-shard (all-uring-1), time-series-ttl (all-uring-1), skewed-multi-shard (all-uring-2), time-series-ttl (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Reproducibility: run 1 and run 2 within tolerance |  | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.31×, scan 1.26×; run 2: row read 1.30×, scan 1.27× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.68 | 0.61 | yes |
| all-uring-1 | group-commit | 0.84 | 0.92 | yes |
| all-uring-1 | skewed-multi-shard | 1.72 | 2.03 | **no** |
| all-uring-1 | sparse-wide | 0.57 | 0.41 | yes |
| all-uring-1 | time-series-ttl | 1.56 | 1.44 | **no** |
| all-uring-1 | ycsb-a | 0.80 | 0.43 | yes |
| all-uring-1 | ycsb-b | 0.31 | 0.47 | yes |
| all-uring-1 | ycsb-c | 0.37 | 0.59 | yes |
| all-uring-1 | ycsb-d | 0.43 | 0.73 | yes |
| all-uring-1 | ycsb-e | 0.71 | 0.71 | yes |
| all-uring-1 | ycsb-f | 0.85 | 0.61 | yes |
| all-uring-2 | adjacency | 0.69 | 0.61 | yes |
| all-uring-2 | group-commit | 0.81 | 0.77 | yes |
| all-uring-2 | skewed-multi-shard | 1.82 | 2.10 | **no** |
| all-uring-2 | sparse-wide | 0.57 | 0.41 | yes |
| all-uring-2 | time-series-ttl | 1.60 | 1.66 | **no** |
| all-uring-2 | ycsb-a | 0.79 | 0.42 | yes |
| all-uring-2 | ycsb-b | 0.29 | 0.42 | yes |
| all-uring-2 | ycsb-c | 0.37 | 0.59 | yes |
| all-uring-2 | ycsb-d | 0.46 | 0.82 | yes |
| all-uring-2 | ycsb-e | 0.70 | 0.71 | yes |
| all-uring-2 | ycsb-f | 0.84 | 0.61 | yes |

## Decisions this window settles

**Row cache (D201):** off (latency step) against on (256 MiB).

| Workload | Throughput off → on | p50 off → on | p99 off → on |
|---|---|---|---|
| ycsb-c | 225535 → 359103 | 3.38 → 0.94 µs | 12.7 → 14.1 µs |
| ycsb-a | 117716 → 120811 | 9.28 → 9.54 µs | 15.6 → 16.5 µs |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 115271 | 9.5 | 16.5 | shard_parks 0.49, shard_wakes 0.49, commit_parks 0.00 |
| shards1 | 168602 | 4.2 | 13.6 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 133500 | 7.3 | 14.7 | shard_parks 0.30, shard_wakes 0.30, commit_parks 0.00 |
| spin100 | 115728 | 9.5 | 16.2 | shard_parks 0.48, shard_wakes 0.48, commit_parks 0.00 |
| spin200 | 116688 | 9.0 | 16.6 | shard_parks 0.41, shard_wakes 0.41, commit_parks 0.01 |
| spin500 | 117451 | 6.7 | 22.8 | shard_parks 0.25, shard_wakes 0.25, commit_parks 0.02 |
| RocksDB | 90707 | 6.4 | 38.7 | - |

**Group sync depth (D207):** durable group commits, ops/s and p99 per client count.

| Depth | 1 clients | 4 clients | 16 clients |
|---|---|---|---|
| 1 | 17206 ops/s, p99 130 µs | 51129 ops/s, p99 132 µs | 106501 ops/s, p99 248 µs |
| 2 | 17526 ops/s, p99 122 µs | 51009 ops/s, p99 138 µs | 113790 ops/s, p99 191 µs |
| unlimited | 17704 ops/s, p99 119 µs | 51381 ops/s, p99 136 µs | 115458 ops/s, p99 188 µs |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 0.99 | 1.02 |
| group-commit | 1.01 | 0.88 |
| skewed-multi-shard | 0.80 | 1.45 |
| sparse-wide | 0.96 | 0.98 |
| time-series-ttl | 0.89 | 1.23 |
| ycsb-a | 0.86 | 1.13 |
| ycsb-b | 0.95 | 1.16 |
| ycsb-c | 1.01 | 1.00 |
| ycsb-d | 1.00 | 1.04 |
| ycsb-e | 1.00 | 0.97 |
| ycsb-f | 0.88 | 1.17 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 0 | 2669 |
| pread | 0 | 16 | 3679 |
| pread | 0 | 16,merge | 2736 |
| pread | 0 | 4 | 3158 |
| pread | 0 | 4,merge | 2466 |
| pread | 1 | 0 | 186 |
| pread | 1 | 16 | 1028 |
| pread | 1 | 16,merge | 1037 |
| pread | 1 | 4 | 380 |
| pread | 1 | 4,merge | 473 |
| uring | 0 | 0 | 2650 |
| uring | 0 | 16 | 3284 |
| uring | 0 | 16,merge | 2957 |
| uring | 0 | 4 | 3057 |
| uring | 0 | 4,merge | 2624 |
| uring | 1 | 0 | 186 |
| uring | 1 | 16 | 1016 |
| uring | 1 | 16,merge | 1082 |
| uring | 1 | 4 | 380 |
| uring | 1 | 4,merge | 480 |

**Mixed commits:** the steps ran at (not recorded), 8f675ed0c433 (a resumed window); see the table below.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | (not recorded) |
| build | 2 | (not recorded) |
| memtable-lookup | 1 | (not recorded) |
| phase2-gate | 3 | (not recorded) |
| phase2-gate-2 | 3 | (not recorded) |
| all-uring-1 | 12 | (not recorded) |
| all-uring-2 | 12 | (not recorded) |
| all-pread | 11 | (not recorded) |
| build | 0 | 8f675ed0c433 |
| latency | 3 | 8f675ed0c433 |
| ycsb-a-shards | 3 | 8f675ed0c433 |
| group-sync-depth | 1 | 8f675ed0c433 |
| row-cache | 0 | 8f675ed0c433 |
| scaling | 5 | 8f675ed0c433 |
| open-latency | 0 | 8f675ed0c433 |
| cold-get | 52 | 8f675ed0c433 |
| cold-scan | 23 | 8f675ed0c433 |
| scan-cache | 0 | 8f675ed0c433 |
| **total** | **131** | |
