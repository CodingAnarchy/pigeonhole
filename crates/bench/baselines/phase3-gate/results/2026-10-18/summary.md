# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** f61510ddabbe5e5357b85d2ebeec36e563052522; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.83 µs, p99 5.79 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 339 ns (262k entries/miss (lower bound) 261 ns, 1M entries/hit (get) 482 ns, 1M entries/miss (lower bound) 403 ns, 10k entries/hit (get) 127 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.052 device reads, 1.048 cache misses per get; p50 88 µs, p99 194 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 5.69M cells/s at 16 shards (355K per core); p50 168 µs, p99 222 µs; busiest shard 6%; 2.0M commits in 1.4 s, 80 flushes, 16 compactions; in the engine p50 131 µs, p99 197 µs, p99.9 786 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 24 µs, p99 83 µs | **FAIL** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 5.94M cells/s (+4% vs pinned); p50 163 µs, p99 225 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 3.60M cells/s (-37% vs 16); p50 63 µs, p99 103 µs, p99.9 154 µs; in the engine p99 98 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 4.58M cells/s (-19% vs 16); p50 107 µs, p99 138 µs, p99.9 313 µs; in the engine p99 131 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 5.77M cells/s at 16 shards (361K per core); p50 168 µs, p99 221 µs; 2.0M commits in 1.4 s, 81 flushes, 16 compactions; in the engine p50 131 µs, p99 197 µs, p99.9 393 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 3.41M cells/s at 16 shards; p50 251 µs, p99 516 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 197 µs, p99 393 µs, p99.9 4194 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 68K cells/s, p99 120 µs, 4 thr: 263K cells/s, p99 147 µs, 16 thr: 584K cells/s, p99 201 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 5.11 GB/s (34330 misses, 382261 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.42 ms (p99 5.77), uring: 3.15 ms (p99 5.95) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 0.99; N=4: 1.12 (busiest 25%; 8.7 s, stalled 0% of shard time, 163 flushes, 43 compactions, max 48.1 ms); N=8: 0.94 (busiest 13%; 8.1 s, stalled 0% of shard time, 247 flushes, 61 compactions, max 112.4 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.61 (busiest 7%; 8.1 s, stalled 0% of shard time, 334 flushes, 76 compactions, max 240.6 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 0.99; N=4: 1.10; N=8: 0.89; N=16: 0.59 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 0.98; N=8: 0.77; N=16: 0.46 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 27.7% (max 31.2%), settled 23 s, 0 changes, 120K of 120K writes/s; N=8: busiest 15.4% (max 15.6%), settled 21 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.5% (max 7.8%), settled 29 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | outside: ycsb-d (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | all-uring-1: 478K ops/s, RocksDB 1.19× throughput, p99 1.30×; tablets on/off 1.06 (packing off); wasted rounds 0.013 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 36059, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.013); Busy 0; cross-shard p99 137 µs (≤ 2 ms), 1.30× RocksDB's batch p99; all-uring-2: 477K ops/s, RocksDB 1.19× throughput, p99 1.26×; tablets on/off 0.87 (packing off); wasted rounds 0.012 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 34062, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.012); Busy 0; cross-shard p99 138 µs (≤ 2 ms), 1.27× RocksDB's batch p99 | **pass** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 4.07×/p99 6.08×); cross-shard-contention (rocksdb 1.37×/p99 2.72×); counters (rocksdb 4.84×/p99 12.72×, sqlite 0.76×/p99 127.16×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 455K ops/s, RocksDB 1.37× throughput, p99 2.72×; tablets on/off 0.93 (packing off); wasted rounds 0.010 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 157308, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.010); Busy 0; cross-shard p99 112 µs (≤ 2 ms), 2.73× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.55×/p99 4.72×); ycsb-f (sqlite 0.12×/p99 1.92×); cross-shard-contention (sqlite 0.18×/p99 2.24×); counters (rocksdb 0.96×/p99 5.50×, sqlite 0.25×/p99 38.63×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 103K ops/s, RocksDB 1.03× throughput, p99 1.18×; tablets on/off 0.98 (packing off); wasted rounds 0.004 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 13027, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.004); Busy 0; cross-shard p99 254 µs (≤ 1 ms), 1.20× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 119K ops/s, p99 852 µs; rocksdb 0.31× throughput, p99 0.41×, sqlite 0.09× throughput, p99 0.03× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 121K ops/s, p99 885 µs; rocksdb 0.24× throughput, p99 0.09×, sqlite 0.07× throughput, p99 0.03× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 593K ops/s, p50 27 µs, p99 528 µs; 0.54× the synchronous run's throughput (p99 11.47×, queued by design) | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 703K ops/s, p50 158 µs, p99 889 µs; 0.64× the synchronous run's throughput (p99 19.29×, queued by design) | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 895K ops/s, p50 12 µs, p99 373 µs; 0.42× the synchronous run's throughput (p99 13.18×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 948K ops/s, p50 112 µs, p99 672 µs; 0.44× the synchronous run's throughput (p99 23.75×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 537K ops/s, p50 62 µs, p99 489 µs; 1.24× the synchronous run's throughput (p99 4.53×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 552K ops/s, p50 229 µs, p99 1114 µs; 1.27× the synchronous run's throughput (p99 10.31×, queued by design) | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 855K ops/s, p50 12 µs, p99 391 µs; 0.37× the synchronous run's throughput (p99 14.15×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 949K ops/s, p50 112 µs, p99 672 µs; 0.41× the synchronous run's throughput (p99 24.30×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 837K commits/s, p50 165 µs, p99 659 µs; tablets on 16 of 16 shards after a 16 s warmup; 7562954 two-phase rounds, 141051 refused; 0 commits retried after Busy, 587 write stalls (1453 ms); 0.21× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 388K commits/s, p50 330 µs, p99 3981 µs; tablets on 16 of 16 shards after a 24 s warmup; 3561989 two-phase rounds, 43560 refused; 0 commits retried after Busy, 4 write stalls (112 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 303 µs (881 snapshots), durable-snapshot scans p99 295 µs (894); durable backup 35009 ms, compaction 2335 ms; close 836 ms; foreground p99 82 µs, 1.79× the judged ycsb-a's | - | `contended-buffered-ycsb-a-riders.json` |
| Contended ycsb-a with riders, default durability, GroupSync (reported, not judged; D247) | snapshot scans p99 272 µs (621 snapshots), durable-snapshot scans p99 264 µs (609); durable backup 37699 ms, compaction 1487 ms; close 540 ms; foreground p99 123 µs, 1.14× the judged ycsb-a's | - | `contended-default-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 5488 ops/s, p99 40108 µs; 1.177 conflicts per transfer, 0.797 pending-share overlaps retried in the engine (D249); rocksdb 91.78× throughput, p99 122.40×, sqlite 7.67× throughput, p99 131.44×; **> 3× worse than cross-shard-contention against RocksDB (91.78× vs 1.37×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 82K ops/s, p99 1901 µs; rocksdb 0.40× throughput, p99 0.56×, sqlite 1.05× throughput, p99 0.60× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 136K ops/s, p99 1286 µs; rocksdb 0.41× throughput, p99 0.48×, sqlite 0.18× throughput, p99 0.26× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 24K ops/s, p99 6390 µs; rocksdb 1.03× throughput, p99 0.47×, sqlite 0.46× throughput, p99 0.23×; **> 3× worse than time-series-ttl against RocksDB (1.03× vs 0.32×): file an issue** | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 6178 ops/s, p99 36962 µs; 1.706 conflicts per transfer, 1.330 pending-share overlaps retried in the engine (D249); rocksdb 8.74× throughput, p99 11.34×, sqlite 3.13× throughput, p99 360.96×; **> 3× worse than cross-shard-contention against RocksDB (8.74× vs 1.03×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 78K ops/s, p99 1901 µs; rocksdb 0.49× throughput, p99 0.54×, sqlite 0.69× throughput, p99 0.59× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 112K ops/s, p99 1606 µs; rocksdb 0.44× throughput, p99 0.59×, sqlite 0.24× throughput, p99 0.37× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 18K ops/s, p99 5046 µs; rocksdb 1.24× throughput, p99 0.37×, sqlite 0.71× throughput, p99 0.27× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 129 s); buffered-adjacency-prefix (rocksdb, 132 s); buffered-adjacency-prefix (sqlite-eav, 134 s); buffered-counters (pigeonhole, 176 s); buffered-sparse-wide (rocksdb, 171 s); buffered-time-series-range (pigeonhole, 133 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 158 s); buffered-transfer (pigeonhole, 135 s); default-adjacency-prefix (pigeonhole, 131 s); default-adjacency-prefix (rocksdb, 132 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 9 metrics outside tolerance; 2 with different background work (D233) | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.39×, scan 1.20×; run 2: row read 1.37×, scan 1.20× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.56 | 0.50 | yes |
| all-uring-1 | cross-shard-contention | 1.19 | 1.30 | yes |
| all-uring-1 | group-commit | 0.50 | 0.74 | yes |
| all-uring-1 | skewed-multi-shard | 0.97 | 1.09 | yes |
| all-uring-1 | sparse-wide | 0.69 | 0.45 | yes |
| all-uring-1 | time-series-ttl | 1.21 | 1.05 | yes |
| all-uring-1 | ycsb-a | 0.78 | 0.54 | yes |
| all-uring-1 | ycsb-b | 0.38 | 1.32 | yes |
| all-uring-1 | ycsb-c | 0.24 | 0.65 | yes |
| all-uring-1 | ycsb-d | 0.45 | 0.98 | yes |
| all-uring-1 | ycsb-e | 0.83 | 0.98 | yes |
| all-uring-1 | ycsb-f | 0.96 | 0.69 | yes |
| all-uring-2 | adjacency | 0.58 | 0.51 | yes |
| all-uring-2 | cross-shard-contention | 1.19 | 1.26 | yes |
| all-uring-2 | group-commit | 0.56 | 0.78 | yes |
| all-uring-2 | skewed-multi-shard | 0.99 | 1.13 | yes |
| all-uring-2 | sparse-wide | 0.67 | 0.45 | yes |
| all-uring-2 | time-series-ttl | 1.22 | 1.03 | yes |
| all-uring-2 | ycsb-a | 0.60 | 0.35 | yes |
| all-uring-2 | ycsb-b | 0.33 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.44 | 1.14 | yes |
| all-uring-2 | ycsb-d | 0.50 | 2.10 | **no** |
| all-uring-2 | ycsb-e | 0.82 | 0.93 | yes |
| all-uring-2 | ycsb-f | 1.28 | 1.08 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 1105067 | 0.62, 0.67 | 0.08, 0.04 | yes | 433961 | 0.44, 0.51 | 0.09, 1.37 | yes |
| ycsb-b | 1582279 | 0.39, 0.70 | 0.14, 0.03 | yes | 1219053 | 0.36, 0.45 | 0.13, 0.07 | yes |
| ycsb-c | 2137543 | 0.67, 1.24 | 0.10, 0.16 | yes | 2334108 | 0.68, 1.28 | 0.09, 0.15 | yes |
| ycsb-e | 32984 | 4.07, 6.08 | 1.36, 0.98 | **no** | 30520 | 3.55, 4.72 | 1.37, 0.50 | **no** |
| ycsb-f | 540502 | 1.17, 1.11 | 0.15, 0.07 | yes | 309389 | 0.54, 0.63 | 0.12, 1.92 | **no** |
| sparse-wide | 79052 | 0.96, 0.75 | 0.51, 0.63 | yes | 71126 | 0.84, 0.72 | 0.48, 0.30 | yes |
| time-series-ttl | 1193521 | 0.32, 0.15 | 0.06, 0.02 | yes | 531855 | 0.46, 0.57 | 0.08, 1.09 | yes |
| cross-shard-contention | 455133 | 1.37, 2.72 | 0.08, 0.10 | **no** | 103125 | 1.03, 1.18 | 0.18, 2.24 | **no** |
| counters | 111583 | 4.84, 12.72 | 0.76, 127.16 | **no** | 109938 | 0.96, 5.50 | 0.25, 38.63 | **no** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 377748 → 575048 | 2.33 → 0.83 µs | 5.0 → 5.8 µs | 75% |
| ycsb-a | 126416 → 130370 | 8.06 → 8.45 µs | 14.3 → 15.1 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 133026 | 8.3 | 16.1 | shard_parks 0.47, shard_wakes 0.47, commit_parks 0.00 |
| shards1 | 189398 | 4.5 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 145386 | 7.5 | 15.2 | shard_parks 0.30, shard_wakes 0.30, commit_parks 0.00 |
| spin100 | 142781 | 7.8 | 14.5 | shard_parks 0.38, shard_wakes 0.38, commit_parks 0.00 |
| spin200 | 151848 | 6.1 | 16.3 | shard_parks 0.26, shard_wakes 0.26, commit_parks 0.00 |
| spin500 | 153031 | 5.4 | 17.9 | shard_parks 0.08, shard_wakes 0.08, commit_parks 0.00 |
| RocksDB | 103148 | 6.0 | 28.4 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.00 | 0.99 |
| cross-shard-contention | 0.93 | 1.01 |
| group-commit | 1.13 | 1.00 |
| skewed-multi-shard | 1.03 | 1.05 |
| sparse-wide | 1.01 | 0.98 |
| time-series-ttl | 1.02 | 1.01 |
| ycsb-a | 0.94 | 1.04 |
| ycsb-b | 1.00 | 1.02 |
| ycsb-c | 0.98 | 1.04 |
| ycsb-d | 0.99 | 1.00 |
| ycsb-e | 1.13 | 0.86 |
| ycsb-f | 1.03 | 0.66 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3201 |
| pread | 1 | 16 (D222) | 1031 |
| uring | 0 | 16 (D222) | 3074 |
| uring | 1 | 16 (D222) | 1018 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 431690 | 194 | 400 | 224 | 1216 MiB | 1216 MiB | 339 ms | 284 µs |
| 4× budget | 396612 | 110 | 142 | 220 | 1472 MiB | 1472 MiB | 500 ms | 2618 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | f61510ddabbe |
| build | 2 | f61510ddabbe |
| memtable-lookup | 1 | f61510ddabbe |
| phase2-gate | 3 | f61510ddabbe |
| phase2-gate-2 | 3 | f61510ddabbe |
| all-uring-1 | 17 | f61510ddabbe |
| all-uring-2 | 17 | f61510ddabbe |
| all-pread | 17 | f61510ddabbe |
| latency | 5 | f61510ddabbe |
| ycsb-a-shards | 3 | f61510ddabbe |
| row-cache | 0 | f61510ddabbe |
| scaling | 9 | f61510ddabbe |
| placement | 2 | f61510ddabbe |
| contended | 200 | f61510ddabbe |
| open-latency | 0 | f61510ddabbe |
| wal-pin | 0 | f61510ddabbe |
| cold-get | 37 | f61510ddabbe |
| cold-scan | 2 | f61510ddabbe |
| scan-cache | 0 | f61510ddabbe |
| **total** | **318** | |
