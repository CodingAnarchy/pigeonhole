# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 8654ad6924a937a4ca36ad9ab999c86ae0544e33; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.80 µs, p99 5.63 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 339 ns (262k entries/miss (lower bound) 265 ns, 1M entries/hit (get) 482 ns, 1M entries/miss (lower bound) 401 ns, 10k entries/hit (get) 128 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.028 device reads, 1.025 cache misses per get; p50 87 µs, p99 181 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 9.75M cells/s at 16 shards (609K per core); p50 84 µs, p99 138 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 524 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 25 µs, p99 77 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 9.22M cells/s (-5% vs pinned); p50 79 µs, p99 162 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.84M cells/s (-50% vs 16); p50 46 µs, p99 78 µs, p99.9 113 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.78M cells/s (-20% vs 16); p50 53 µs, p99 91 µs, p99.9 182 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 10.12M cells/s at 16 shards (632K per core); p50 72 µs, p99 150 µs; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 1049 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 5.02M cells/s at 16 shards; p50 157 µs, p99 377 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 131 µs, p99 262 µs, p99.9 4194 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 73K cells/s, p99 117 µs, 4 thr: 233K cells/s, p99 148 µs, 16 thr: 450K cells/s, p99 309 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.59 GB/s (0 misses, 270436 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 2.97 ms (p99 4.97), uring: 2.60 ms (p99 5.04) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 0.90; N=4: 1.10 (busiest 25%; 8.7 s, stalled 0% of shard time, 163 flushes, 42 compactions, max 22.5 ms); N=8: 0.99 (busiest 13%; 8.4 s, stalled 0% of shard time, 287 flushes, 76 compactions, max 48.8 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.70 (busiest 7%; 8.2 s, stalled 0% of shard time, 394 flushes, 91 compactions, max 59.1 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 0.90; N=4: 1.10; N=8: 1.00; N=16: 0.69 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.03; N=8: 0.90; N=16: 0.59 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 28.0% (max 31.2%), settled 21 s, 0 changes, 120K of 120K writes/s; N=8: busiest 14.3% (max 15.6%), settled 16 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.7% (max 7.8%), settled 26 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | outside: skewed-multi-shard (all-uring-1), skewed-multi-shard (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Reproducibility: run 1 and run 2 within tolerance |  | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.32×, scan 1.17×; run 2: row read 1.47×, scan 1.25× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.60 | 0.53 | yes |
| all-uring-1 | group-commit | 0.83 | 1.23 | yes |
| all-uring-1 | skewed-multi-shard | 1.93 | 2.96 | **no** |
| all-uring-1 | sparse-wide | 0.68 | 0.46 | yes |
| all-uring-1 | time-series-ttl | 1.17 | 1.14 | yes |
| all-uring-1 | ycsb-a | 0.58 | 0.44 | yes |
| all-uring-1 | ycsb-b | 0.30 | 0.48 | yes |
| all-uring-1 | ycsb-c | 0.43 | 1.09 | yes |
| all-uring-1 | ycsb-d | 0.45 | 0.97 | yes |
| all-uring-1 | ycsb-e | 0.88 | 0.97 | yes |
| all-uring-1 | ycsb-f | 0.69 | 0.58 | yes |
| all-uring-2 | adjacency | 0.62 | 0.56 | yes |
| all-uring-2 | group-commit | 0.83 | 0.99 | yes |
| all-uring-2 | skewed-multi-shard | 1.66 | 1.92 | **no** |
| all-uring-2 | sparse-wide | 0.69 | 0.47 | yes |
| all-uring-2 | time-series-ttl | 1.17 | 1.14 | yes |
| all-uring-2 | ycsb-a | 0.48 | 0.49 | yes |
| all-uring-2 | ycsb-b | 0.33 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.44 | 1.10 | yes |
| all-uring-2 | ycsb-d | 0.46 | 0.97 | yes |
| all-uring-2 | ycsb-e | 0.80 | 0.83 | yes |
| all-uring-2 | ycsb-f | 0.72 | 0.66 | yes |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 381569 → 586709 | 2.32 → 0.80 µs | 4.6 → 5.6 µs | 74% |
| ycsb-a | 179934 → 183992 | 4.54 → 4.80 µs | 12.7 → 13.8 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 175743 | 4.7 | 14.2 | shard_parks 0.07, shard_wakes 0.07, commit_parks 0.00 |
| shards1 | 199417 | 4.5 | 13.5 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 173496 | 4.6 | 14.1 | shard_parks 0.06, shard_wakes 0.06, commit_parks 0.00 |
| spin100 | 193629 | 4.6 | 13.8 | shard_parks 0.01, shard_wakes 0.01, commit_parks 0.00 |
| spin200 | 190659 | 4.6 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 196481 | 4.6 | 13.5 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 108275 | 5.9 | 27.1 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.03 | 0.96 |
| group-commit | 0.98 | 1.24 |
| skewed-multi-shard | 0.69 | 2.60 |
| sparse-wide | 1.02 | 0.96 |
| time-series-ttl | 1.02 | 0.96 |
| ycsb-a | 0.92 | 1.02 |
| ycsb-b | 1.01 | 0.96 |
| ycsb-c | 0.96 | 1.04 |
| ycsb-d | 0.99 | 1.01 |
| ycsb-e | 1.00 | 0.92 |
| ycsb-f | 0.93 | 1.09 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3804 |
| pread | 1 | 16 (D222) | 1056 |
| uring | 0 | 16 (D222) | 3364 |
| uring | 1 | 16 (D222) | 1046 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 357355 | 194 | 332 | 218 | 1152 MiB | 1152 MiB | 338 ms | 901 µs |
| 4× budget | 348088 | 0 | 0 | 212 | 1344 MiB | 1344 MiB | 663 ms | 3284 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

**Mixed commits:** the steps ran at 8654ad6924a9, 93052c78c110 (a resumed window); see the table below.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 8654ad6924a9 |
| build | 2 | 8654ad6924a9 |
| memtable-lookup | 1 | 8654ad6924a9 |
| phase2-gate | 3 | 8654ad6924a9 |
| phase2-gate-2 | 3 | 8654ad6924a9 |
| all-uring-1 | 12 | 8654ad6924a9 |
| all-uring-2 | 12 | 8654ad6924a9 |
| all-pread | 12 | 8654ad6924a9 |
| latency | 4 | 8654ad6924a9 |
| ycsb-a-shards | 2 | 8654ad6924a9 |
| row-cache | 0 | 8654ad6924a9 |
| scaling | 8 | 8654ad6924a9 |
| placement | 1 | 8654ad6924a9 |
| open-latency | 0 | 8654ad6924a9 |
| wal-pin | 0 | 8654ad6924a9 |
| cold-scan | 2 | 8654ad6924a9 |
| scan-cache | 0 | 8654ad6924a9 |
| report | 0 | 8654ad6924a9 |
| all-uring-1 | 12 | 8654ad6924a9 |
| all-uring-2 | 12 | 8654ad6924a9 |
| report | 0 | 8654ad6924a9 |
| build | 2 | 93052c78c110 |
| cold-get | 40 | 93052c78c110 |
| **total** | **128** | |
