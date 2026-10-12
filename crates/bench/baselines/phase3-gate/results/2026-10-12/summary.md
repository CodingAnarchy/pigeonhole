# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** a8b167e6a73d0fd8fe3494dde52cbc257af2c107; **scale:** full
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.86 µs, p99 7.23 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 1M entries: ≤ 300 ns (#17) | 1M hit 484 ns (1M entries/miss (lower bound) 404 ns, 10k entries/hit (get) 128 ns, 10k entries/miss (lower bound) 106 ns) | **FAIL** | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.106 device reads, 1.105 cache misses per get; p50 88 µs, p99 205 µs; block cache 1024 MiB | **FAIL** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 7.00M cells/s at 16 shards (438K per core); p50 116 µs, p99 241 µs; busiest shard 6%; 2.0M commits in 1.1 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 197 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 25 µs, p99 75 µs | **FAIL** | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 3.28M cells/s (-53% vs 16); p50 70 µs, p99 133 µs, p99.9 688 µs; in the engine p99 98 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 5.09M cells/s (-27% vs 16); p50 85 µs, p99 162 µs, p99.9 762 µs; in the engine p99 98 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 7.96M cells/s at 16 shards (498K per core); p50 105 µs, p99 188 µs; 2.0M commits in 1.0 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 197 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 3.67M cells/s at 16 shards; p50 241 µs, p99 455 µs; 0.2M commits in 0.2 s, 15 flushes, 0 compactions; in the engine p50 131 µs, p99 262 µs, p99.9 524 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 81K cells/s, p99 102 µs, 4 thr: 219K cells/s, p99 132 µs, 16 thr: 470K cells/s, p99 186 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.02 GB/s (0 misses, 271775 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 2.68 ms (p99 5.00), uring: 2.45 ms (p99 5.50) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N | N=4: 0.88 (busiest 32%; 8.3 s, stalled 5% of shard time, 123 flushes, 30 compactions, max 34.1 ms); N=8: 0.73 (busiest 17%; 8.3 s, stalled 3% of shard time, 205 flushes, 51 compactions, max 19.3 ms); N=16: 0.54 (busiest 8%; 8.4 s, stalled 2% of shard time, 310 flushes, 79 compactions, max 38.9 ms) | **FAIL** | `scaling-N.json` |
| Within 1.5× of RocksDB on every workload | all within | **pass** | `all-uring-1.json, all-uring-2.json` |
| Reproducibility: run 1 and run 2 within tolerance |  | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.33×, scan 1.18×; run 2: row read 1.38×, scan 1.25× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.70 | 0.68 | yes |
| all-uring-1 | group-commit | 0.84 | 0.90 | yes |
| all-uring-1 | skewed-multi-shard | 1.12 | 1.14 | yes |
| all-uring-1 | sparse-wide | 0.62 | 0.43 | yes |
| all-uring-1 | time-series-ttl | 1.15 | 1.15 | yes |
| all-uring-1 | ycsb-a | 0.52 | 0.40 | yes |
| all-uring-1 | ycsb-b | 0.23 | 0.37 | yes |
| all-uring-1 | ycsb-c | 0.24 | 0.61 | yes |
| all-uring-1 | ycsb-d | 0.39 | 0.61 | yes |
| all-uring-1 | ycsb-e | 0.84 | 1.01 | yes |
| all-uring-1 | ycsb-f | 0.61 | 0.46 | yes |
| all-uring-2 | adjacency | 0.65 | 0.58 | yes |
| all-uring-2 | group-commit | 0.78 | 0.78 | yes |
| all-uring-2 | skewed-multi-shard | 1.06 | 1.09 | yes |
| all-uring-2 | sparse-wide | 0.63 | 0.43 | yes |
| all-uring-2 | time-series-ttl | 1.14 | 1.14 | yes |
| all-uring-2 | ycsb-a | 0.46 | 0.39 | yes |
| all-uring-2 | ycsb-b | 0.19 | 0.35 | yes |
| all-uring-2 | ycsb-c | 0.23 | 0.59 | yes |
| all-uring-2 | ycsb-d | 0.40 | 0.62 | yes |
| all-uring-2 | ycsb-e | 0.78 | 0.87 | yes |
| all-uring-2 | ycsb-f | 0.59 | 0.47 | yes |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 294961 → 496566 | 3.04 → 0.86 µs | 6.3 → 7.2 µs | 74% |
| ycsb-a | 157478 → 184332 | 4.29 → 3.95 µs | 14.6 → 14.5 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 164897 | 4.1 | 15.8 | shard_parks 0.06, shard_wakes 0.06, commit_parks 0.00 |
| shards1 | 169571 | 3.6 | 14.8 | shard_parks 0.00, shard_wakes 0.01, commit_parks 0.00 |
| shards4 | 182604 | 3.8 | 14.9 | shard_parks 0.06, shard_wakes 0.06, commit_parks 0.00 |
| spin100 | 179315 | 3.8 | 15.0 | shard_parks 0.01, shard_wakes 0.01, commit_parks 0.00 |
| spin200 | 180044 | 3.8 | 15.6 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 183626 | 3.9 | 14.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 91944 | 6.2 | 37.1 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 0.97 | 1.08 |
| group-commit | 0.97 | 0.93 |
| skewed-multi-shard | 1.20 | 0.96 |
| sparse-wide | 1.02 | 0.98 |
| time-series-ttl | 1.02 | 1.02 |
| ycsb-a | 0.92 | 1.06 |
| ycsb-b | 0.79 | 1.02 |
| ycsb-c | 1.00 | 1.00 |
| ycsb-d | 0.98 | 1.01 |
| ycsb-e | 0.97 | 1.09 |
| ycsb-f | 0.96 | 1.04 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3617 |
| pread | 1 | 16 (D222) | 998 |
| uring | 0 | 16 (D222) | 3336 |
| uring | 1 | 16 (D222) | 1023 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 416600 | 188 | 320 | 225 | 1152 MiB | 1152 MiB | 460 ms | 1535 µs |
| 4× budget | 391598 | 0 | 0 | 210 | 1344 MiB | 1344 MiB | 573 ms | 1304 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | a8b167e6a73d |
| build | 0 | a8b167e6a73d |
| memtable-lookup | 1 | a8b167e6a73d |
| phase2-gate | 3 | a8b167e6a73d |
| phase2-gate-2 | 3 | a8b167e6a73d |
| all-uring-1 | 11 | a8b167e6a73d |
| all-uring-2 | 11 | a8b167e6a73d |
| all-pread | 11 | a8b167e6a73d |
| latency | 3 | a8b167e6a73d |
| ycsb-a-shards | 2 | a8b167e6a73d |
| row-cache | 0 | a8b167e6a73d |
| scaling | 5 | a8b167e6a73d |
| open-latency | 0 | a8b167e6a73d |
| wal-pin | 0 | a8b167e6a73d |
| cold-get | 42 | a8b167e6a73d |
| cold-scan | 2 | a8b167e6a73d |
| scan-cache | 0 | a8b167e6a73d |
| **total** | **94** | |
