# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 234c5c2ff10809b5cd5c1706901b45947db28783; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.80 µs, p99 5.73 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 339 ns (262k entries/miss (lower bound) 263 ns, 1M entries/hit (get) 485 ns, 1M entries/miss (lower bound) 410 ns, 10k entries/hit (get) 129 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O |  | not run | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 9.54M cells/s at 16 shards (596K per core); p50 87 µs, p99 143 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 524 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 24 µs, p99 72 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 9.45M cells/s (-1% vs pinned); p50 85 µs, p99 162 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.65M cells/s (-51% vs 16); p50 48 µs, p99 80 µs, p99.9 124 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.02M cells/s (-26% vs 16); p50 62 µs, p99 98 µs, p99.9 173 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 10.38M cells/s at 16 shards (649K per core); p50 86 µs, p99 153 µs; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 197 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 4.60M cells/s at 16 shards; p50 165 µs, p99 868 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 131 µs, p99 786 µs, p99.9 3146 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 67K cells/s, p99 125 µs, 4 thr: 222K cells/s, p99 165 µs, 16 thr: 659K cells/s, p99 178 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core |  | not run | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.42 ms (p99 6.19), uring: 2.93 ms (p99 5.13) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 1.05; N=4: 1.09 (busiest 25%; 8.8 s, stalled 0% of shard time, 164 flushes, 43 compactions, max 49.0 ms); N=8: 0.98 (busiest 13%; 8.6 s, stalled 0% of shard time, 287 flushes, 74 compactions, max 63.6 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.63 (busiest 7%; 8.0 s, stalled 0% of shard time, 356 flushes, 90 compactions, max 49.9 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 1.00; N=4: 1.09; N=8: 0.98; N=16: 0.63 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.01; N=8: 0.82; N=16: 0.59 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 27.8% (max 31.2%), settled 21 s, 0 changes, 120K of 120K writes/s; N=8: busiest 15.4% (max 15.6%), settled 13 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.5% (max 7.8%), settled 19 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | outside: cross-shard-contention (all-uring-1), cross-shard-contention (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | all-uring-1: 222K ops/s, RocksDB 2.57× throughput, p99 2.19×; tablets on/off 0.99 (packing off); wasted rounds 0.008 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 10044, move raise 0, moved 94, room 92, pending overlap 0 (D249); seqno inference 0.008); Busy 0; cross-shard p99 223 µs (≤ 2 ms), 2.16× RocksDB's batch p99; all-uring-2: 245K ops/s, RocksDB 2.34× throughput, p99 2.07×; tablets on/off 0.98 (packing off); wasted rounds 0.007 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 8035, move raise 0, moved 84, room 113, pending overlap 0 (D249); seqno inference 0.007); Busy 0; cross-shard p99 211 µs (≤ 2 ms), 2.06× RocksDB's batch p99 | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.56×/p99 5.20×); ycsb-f (rocksdb 1.27×/p99 1.58×); cross-shard-contention (rocksdb 2.00×/p99 4.14×); counters (rocksdb 3.04×/p99 6.32×, sqlite 0.49×/p99 67.05×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 317K ops/s, RocksDB 2.00× throughput, p99 4.14×; tablets on/off 0.92 (packing off); wasted rounds 0.018 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 280124, move raise 0, moved 155, room 147, pending overlap 0 (D249); seqno inference 0.018); Busy 0; cross-shard p99 155 µs (≤ 2 ms), 4.05× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.02×/p99 3.91×); ycsb-f (sqlite 0.13×/p99 2.01×); counters (rocksdb 1.29×/p99 7.58×, sqlite 0.33×/p99 53.41×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 98K ops/s, RocksDB 1.10× throughput, p99 1.18×; tablets on/off 0.97 (packing off); wasted rounds 0.005 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 17061, move raise 0, moved 10, room 0, pending overlap 0 (D249); seqno inference 0.005); Busy 0; cross-shard p99 260 µs (≤ 1 ms), 1.20× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 59K ops/s, p99 905 µs; rocksdb 0.56× throughput, p99 0.40×, sqlite 0.19× throughput, p99 0.03× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 83K ops/s, p99 1737 µs; rocksdb 0.24× throughput, p99 0.09×, sqlite 0.10× throughput, p99 0.05× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 107K ops/s, p50 165 µs, p99 2785 µs; 0.11× the synchronous run's throughput (p99 65.15×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 39K ops/s, p50 144 µs, p99 24379 µs; 0.04× the synchronous run's throughput (p99 570.26×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 900K ops/s, p50 12 µs, p99 371 µs; 0.44× the synchronous run's throughput (p99 12.99×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 929K ops/s, p50 114 µs, p99 688 µs; 0.45× the synchronous run's throughput (p99 24.11×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 443K ops/s, p50 71 µs, p99 926 µs; 1.02× the synchronous run's throughput (p99 9.00×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 21K ops/s, p50 1376 µs, p99 28836 µs; 0.05× the synchronous run's throughput (p99 280.20×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 893K ops/s, p50 12 µs, p99 375 µs; 0.40× the synchronous run's throughput (p99 13.62×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 943K ops/s, p50 112 µs, p99 680 µs; 0.43× the synchronous run's throughput (p99 24.71×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 844K commits/s, p50 166 µs, p99 528 µs; tablets on 16 of 16 shards after a 13 s warmup; 7564801 two-phase rounds, 139361 refused; 0 commits retried after Busy, 16 write stalls (1166 ms); 0.20× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 358K commits/s, p50 365 µs, p99 3998 µs; tablets on 16 of 16 shards after a 27 s warmup; 3565010 two-phase rounds, 47070 refused; 0 commits retried after Busy, 5 write stalls (179 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 185 µs (454 snapshots), durable-snapshot scans p99 184 µs (454); durable backup 49322 ms, compaction 2877 ms; close 596 ms; foreground p99 97518 µs, 2281.06× the judged ycsb-a's; **> 3× the judged ycsb-a's p99: file an issue** | - | `contended-buffered-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 6472 ops/s, p99 10027 µs; 0.917 conflicts per transfer, 0.398 pending-share overlaps retried in the engine (D249); rocksdb 77.60× throughput, p99 30.79×, sqlite 6.45× throughput, p99 19.90×; **> 3× worse than cross-shard-contention against RocksDB (77.60× vs 2.00×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 91K ops/s, p99 1753 µs; rocksdb 0.36× throughput, p99 0.49×, sqlite 0.94× throughput, p99 0.55× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 169K ops/s, p99 1311 µs; rocksdb 0.32× throughput, p99 0.49×, sqlite 0.14× throughput, p99 0.27× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 29K ops/s, p99 4751 µs; rocksdb 0.84× throughput, p99 0.34×, sqlite 0.39× throughput, p99 0.17×; **> 3× worse than time-series-ttl against RocksDB (0.84× vs 0.27×): file an issue** | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 5115 ops/s, p99 36700 µs; 5.698 conflicts per transfer, 1.410 pending-share overlaps retried in the engine (D249); rocksdb 10.67× throughput, p99 11.37×, sqlite 3.81× throughput, p99 347.96×; **> 3× worse than cross-shard-contention against RocksDB (10.67× vs 1.10×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 84K ops/s, p99 1753 µs; rocksdb 0.44× throughput, p99 0.49×, sqlite 0.64× throughput, p99 0.55× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 137K ops/s, p99 1270 µs; rocksdb 0.36× throughput, p99 0.46×, sqlite 0.19× throughput, p99 0.28× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 28K ops/s, p99 5112 µs; rocksdb 0.77× throughput, p99 0.37×, sqlite 0.46× throughput, p99 0.28× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 128 s); buffered-adjacency-prefix (rocksdb, 131 s); buffered-adjacency-prefix (sqlite-eav, 134 s); buffered-sparse-wide (pigeonhole, 176 s); buffered-sparse-wide (rocksdb, 168 s); buffered-time-series-range (pigeonhole, 134 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 160 s); default-adjacency-prefix (rocksdb, 133 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 2 metrics outside tolerance; 2 with different background work (D233) | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.28×, scan 1.11×; run 2: row read 1.26×, scan 1.05× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.57 | 0.50 | yes |
| all-uring-1 | cross-shard-contention | 2.57 | 2.19 | **no** |
| all-uring-1 | group-commit | 0.45 | 0.74 | yes |
| all-uring-1 | skewed-multi-shard | 1.21 | 1.29 | yes |
| all-uring-1 | sparse-wide | 0.64 | 0.42 | yes |
| all-uring-1 | time-series-ttl | 0.97 | 0.79 | yes |
| all-uring-1 | ycsb-a | 0.56 | 0.50 | yes |
| all-uring-1 | ycsb-b | 0.32 | 0.60 | yes |
| all-uring-1 | ycsb-c | 0.41 | 1.09 | yes |
| all-uring-1 | ycsb-d | 0.43 | 0.94 | yes |
| all-uring-1 | ycsb-e | 0.75 | 0.84 | yes |
| all-uring-1 | ycsb-f | 1.07 | 0.93 | yes |
| all-uring-2 | adjacency | 0.57 | 0.51 | yes |
| all-uring-2 | cross-shard-contention | 2.34 | 2.07 | **no** |
| all-uring-2 | group-commit | 0.52 | 0.78 | yes |
| all-uring-2 | skewed-multi-shard | 1.17 | 1.20 | yes |
| all-uring-2 | sparse-wide | 0.70 | 0.45 | yes |
| all-uring-2 | time-series-ttl | 1.00 | 0.95 | yes |
| all-uring-2 | ycsb-a | 0.59 | 0.52 | yes |
| all-uring-2 | ycsb-b | 0.32 | 0.61 | yes |
| all-uring-2 | ycsb-c | 0.41 | 1.09 | yes |
| all-uring-2 | ycsb-d | 0.45 | 0.93 | yes |
| all-uring-2 | ycsb-e | 0.69 | 0.66 | yes |
| all-uring-2 | ycsb-f | 0.92 | 0.81 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 975142 | 0.71, 0.63 | 0.09, 0.04 | yes | 433409 | 0.44, 0.49 | 0.09, 1.21 | yes |
| ycsb-b | 1597901 | 0.38, 0.70 | 0.13, 0.03 | yes | 1280365 | 0.34, 0.45 | 0.13, 0.07 | yes |
| ycsb-c | 2050026 | 0.69, 1.22 | 0.10, 0.16 | yes | 2209431 | 0.72, 1.29 | 0.10, 0.15 | yes |
| ycsb-e | 36176 | 3.56, 5.20 | 1.21, 0.88 | **no** | 35815 | 3.02, 3.91 | 1.16, 0.41 | **no** |
| ycsb-f | 501663 | 1.27, 1.58 | 0.16, 0.10 | **no** | 297062 | 0.57, 0.69 | 0.13, 2.01 | **no** |
| sparse-wide | 69216 | 1.10, 0.92 | 0.60, 0.78 | yes | 68740 | 0.85, 0.76 | 0.49, 0.32 | yes |
| time-series-ttl | 1379600 | 0.27, 0.13 | 0.05, 0.01 | yes | 532970 | 0.46, 0.56 | 0.07, 0.93 | yes |
| cross-shard-contention | 317207 | 2.00, 4.14 | 0.12, 0.14 | **no** | 97790 | 1.10, 1.18 | 0.18, 1.35 | yes |
| counters | 174468 | 3.04, 6.32 | 0.49, 67.05 | **no** | 82189 | 1.29, 7.58 | 0.33, 53.41 | **no** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 385785 → 584819 | 2.22 → 0.80 µs | 4.7 → 5.7 µs | 74% |
| ycsb-a | 167902 → 190936 | 4.80 → 4.51 µs | 13.7 → 14.2 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 185061 | 4.6 | 14.4 | shard_parks 0.00, shard_wakes 0.01, commit_parks 0.00 |
| shards1 | 224882 | 3.7 | 13.6 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 229866 | 3.6 | 12.6 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin100 | 190693 | 4.5 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin200 | 188340 | 4.6 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 186129 | 4.8 | 14.1 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 103682 | 6.0 | 28.4 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.00 | 1.00 |
| cross-shard-contention | 0.96 | 1.05 |
| group-commit | 0.96 | 1.13 |
| skewed-multi-shard | 0.96 | 1.02 |
| sparse-wide | 1.12 | 0.87 |
| time-series-ttl | 1.07 | 0.86 |
| ycsb-a | 0.99 | 1.00 |
| ycsb-b | 1.00 | 0.99 |
| ycsb-c | 1.01 | 0.98 |
| ycsb-d | 0.99 | 1.02 |
| ycsb-e | 1.05 | 1.03 |
| ycsb-f | 1.01 | 1.01 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 414319 | 202 | 414 | 227 | 1216 MiB | 1216 MiB | 142 ms | 522 µs |
| 4× budget | 363415 | 106 | 154 | 219 | 1472 MiB | 1472 MiB | 510 ms | 2642 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 234c5c2ff108 |
| build | 2 | 234c5c2ff108 |
| memtable-lookup | 1 | 234c5c2ff108 |
| phase2-gate | 3 | 234c5c2ff108 |
| phase2-gate-2 | 3 | 234c5c2ff108 |
| all-uring-1 | 17 | 234c5c2ff108 |
| all-uring-2 | 17 | 234c5c2ff108 |
| all-pread | 17 | 234c5c2ff108 |
| latency | 5 | 234c5c2ff108 |
| ycsb-a-shards | 3 | 234c5c2ff108 |
| row-cache | 0 | 234c5c2ff108 |
| scaling | 9 | 234c5c2ff108 |
| placement | 1 | 234c5c2ff108 |
| open-latency | 0 | 234c5c2ff108 |
| wal-pin | 0 | 234c5c2ff108 |
| **total** | **78** | |
