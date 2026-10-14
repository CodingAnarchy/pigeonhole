# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 175c0ffc7318525648d96c3d44c5cdb700cd1920; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.84 µs, p99 6.05 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 339 ns (262k entries/miss (lower bound) 260 ns, 1M entries/hit (get) 485 ns, 1M entries/miss (lower bound) 406 ns, 10k entries/hit (get) 128 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.059 device reads, 1.055 cache misses per get; p50 95 µs, p99 236 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 9.58M cells/s at 16 shards (599K per core); p50 87 µs, p99 138 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 524 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 24 µs, p99 72 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 9.74M cells/s (+2% vs pinned); p50 83 µs, p99 157 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.71M cells/s (-51% vs 16); p50 48 µs, p99 79 µs, p99.9 117 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 6.93M cells/s (-28% vs 16); p50 60 µs, p99 95 µs, p99.9 182 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 10.53M cells/s at 16 shards (658K per core); p50 84 µs, p99 152 µs; 2.0M commits in 0.8 s, 81 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 131 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 4.92M cells/s at 16 shards; p50 163 µs, p99 465 µs; 0.2M commits in 0.2 s, 15 flushes, 0 compactions; in the engine p50 131 µs, p99 393 µs, p99.9 6291 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 70K cells/s, p99 119 µs, 4 thr: 219K cells/s, p99 165 µs, 16 thr: 599K cells/s, p99 207 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.49 GB/s (0 misses, 270554 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 2.96 ms (p99 5.04), uring: 2.33 ms (p99 5.68) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 1.01; N=4: 1.12 (busiest 25%; 8.4 s, stalled 0% of shard time, 163 flushes, 43 compactions, max 15.3 ms); N=8: 1.00 (busiest 13%; 8.3 s, stalled 0% of shard time, 287 flushes, 75 compactions, max 63.5 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.62 (busiest 7%; 8.1 s, stalled 0% of shard time, 356 flushes, 90 compactions, max 67.1 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 1.01; N=4: 1.14; N=8: 1.01; N=16: 0.63 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.01; N=8: 0.91; N=16: 0.60 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 27.3% (max 31.2%), settled 19 s, 0 changes, 120K of 120K writes/s; N=8: busiest 14.8% (max 15.6%), settled 23 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.5% (max 7.8%), settled 24 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | outside: cross-shard-contention (all-uring-1), cross-shard-contention (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off; ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, and ≤ 2 ms at Buffered (GroupSync cap TBD) | all-uring-1: 184K ops/s, RocksDB 3.09× throughput, p99 10.64×; tablets on/off 0.48; wasted rounds 0.004 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 3892, move raise 0, moved 18, room 76, pending overlap 0 (D249); seqno inference 0.004); Busy 0; cross-shard p99 1106 µs (≤ 2 ms), 10.38× RocksDB's batch p99; all-uring-2: 186K ops/s, RocksDB 3.06× throughput, p99 10.77×; tablets on/off 0.48; wasted rounds 0.004 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 3644, move raise 0, moved 12, room 71, pending overlap 0 (D249); seqno inference 0.004); Busy 0; cross-shard p99 1114 µs (≤ 2 ms), 10.46× RocksDB's batch p99 | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-a (rocksdb 0.85×/p99 6.11×); ycsb-e (rocksdb 3.84×/p99 5.45×); ycsb-f (rocksdb 1.44×/p99 2.14×); cross-shard-contention (rocksdb 2.98×/p99 15.31×); counters (rocksdb 3.58×/p99 8.08×, sqlite 0.56×/p99 84.31×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off; ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, and ≤ 2 ms at Buffered (GroupSync cap TBD) | 215K ops/s, RocksDB 2.98× throughput, p99 15.31×; tablets on/off 0.71; wasted rounds 0.012 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 86814, move raise 0, moved 251, room 250, pending overlap 0 (D249); seqno inference 0.012); Busy 0; cross-shard p99 557 µs (≤ 2 ms), 15.01× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.84×/p99 5.09×); ycsb-f (sqlite 0.14×/p99 2.19×); cross-shard-contention (sqlite 0.21×/p99 2.74×); counters (rocksdb 1.03×/p99 5.74×, sqlite 0.26×/p99 42.67×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off; ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, and ≤ 2 ms at Buffered (GroupSync cap TBD) | 86K ops/s, RocksDB 1.25× throughput, p99 1.40×; tablets on/off 0.90; wasted rounds 0.006 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 16041, move raise 0, moved 191, room 13, pending overlap 0 (D249); seqno inference 0.006); Busy 0; cross-shard p99 303 µs (cap TBD), 1.42× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 53K ops/s, p99 1991 µs; rocksdb 0.66× throughput, p99 0.91×, sqlite 0.21× throughput, p99 0.06× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 101K ops/s, p99 2966 µs; rocksdb 0.28× throughput, p99 0.29×, sqlite 0.08× throughput, p99 0.09× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 625K ops/s, p50 22 µs, p99 659 µs; 0.76× the synchronous run's throughput (p99 1.61×, queued by design) | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 103K ops/s, p50 34 µs, p99 17302 µs; 0.13× the synchronous run's throughput (p99 42.24×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 899K ops/s, p50 12 µs, p99 367 µs; 0.39× the synchronous run's throughput (p99 13.32×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 923K ops/s, p50 115 µs, p99 692 µs; 0.40× the synchronous run's throughput (p99 25.15×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 495K ops/s, p50 67 µs, p99 618 µs; 1.22× the synchronous run's throughput (p99 5.59×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 548K ops/s, p50 165 µs, p99 4981 µs; 1.34× the synchronous run's throughput (p99 45.04×, queued by design) | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 847K ops/s, p50 12 µs, p99 395 µs; 0.40× the synchronous run's throughput (p99 13.79×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 926K ops/s, p50 115 µs, p99 692 µs; 0.43× the synchronous run's throughput (p99 24.14×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 741K commits/s, p50 162 µs, p99 610 µs; tablets on 16 of 16 shards after a 22 s warmup; 6772629 two-phase rounds, 130114 refused; 0 commits retried after Busy, 15 write stalls (1048 ms); 0.18× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 354K commits/s, p50 334 µs, p99 3998 µs; tablets on 16 of 16 shards after a 37 s warmup; 3561702 two-phase rounds, 43503 refused; 0 commits retried after Busy, 1 write stalls (65 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 293 µs (983 snapshots), durable-snapshot scans p99 285 µs (972); durable backup 31731 ms, compaction 1340 ms; close 455 ms; foreground p99 102 µs, 0.25× the judged ycsb-a's | - | `contended-buffered-ycsb-a-riders.json` |
| Contended ycsb-a with riders, default durability, GroupSync (reported, not judged; D247) | snapshot scans p99 253 µs (541 snapshots), durable-snapshot scans p99 254 µs (553); durable backup 32592 ms, compaction 771 ms; close 534 ms; foreground p99 118 µs, 1.06× the judged ycsb-a's | - | `contended-default-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 13K ops/s, p99 14746 µs; 1.220 conflicts per transfer, 0.491 pending-share overlaps retried in the engine (D249); rocksdb 38.41× throughput, p99 45.57×, sqlite 3.17× throughput, p99 36.00×; **> 3× worse than cross-shard-contention against RocksDB (38.41× vs 2.98×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 106K ops/s, p99 1384 µs; rocksdb 0.30× throughput, p99 0.39×, sqlite 0.81× throughput, p99 0.44× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 173K ops/s, p99 1335 µs; rocksdb 0.32× throughput, p99 0.50×, sqlite 0.14× throughput, p99 0.27× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 18K ops/s, p99 6324 µs; rocksdb 1.36× throughput, p99 0.45×, sqlite 0.62× throughput, p99 0.22×; **> 3× worse than time-series-ttl against RocksDB (1.36× vs 0.34×): file an issue** | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 7364 ops/s, p99 29360 µs; 1.971 conflicts per transfer, 0.951 pending-share overlaps retried in the engine (D249); rocksdb 7.30× throughput, p99 9.01×, sqlite 2.64× throughput, p99 282.49×; **> 3× worse than cross-shard-contention against RocksDB (7.30× vs 1.25×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 102K ops/s, p99 1327 µs; rocksdb 0.40× throughput, p99 0.39×, sqlite 0.53× throughput, p99 0.41× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 136K ops/s, p99 1212 µs; rocksdb 0.36× throughput, p99 0.45×, sqlite 0.20× throughput, p99 0.28× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 27K ops/s, p99 5571 µs; rocksdb 0.79× throughput, p99 0.41×, sqlite 0.47× throughput, p99 0.29× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 129 s); buffered-adjacency-prefix (rocksdb, 132 s); buffered-adjacency-prefix (sqlite-eav, 134 s); buffered-sparse-wide (rocksdb, 166 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 157 s); default-adjacency-prefix (rocksdb, 132 s); default-sparse-wide (pigeonhole, 176 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (pigeonhole, 132 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) |  | **pass** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **FAIL** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.36×, scan 1.35×; run 2: row read 1.16×, scan 1.32× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.60 | 0.54 | yes |
| all-uring-1 | cross-shard-contention | 3.09 | 10.64 | **no** |
| all-uring-1 | group-commit | 0.52 | 0.82 | yes |
| all-uring-1 | skewed-multi-shard | 1.42 | 1.26 | yes |
| all-uring-1 | sparse-wide | 0.67 | 0.47 | yes |
| all-uring-1 | time-series-ttl | 1.01 | 0.95 | yes |
| all-uring-1 | ycsb-a | 0.56 | 0.48 | yes |
| all-uring-1 | ycsb-b | 0.32 | 0.59 | yes |
| all-uring-1 | ycsb-c | 0.41 | 1.10 | yes |
| all-uring-1 | ycsb-d | 0.47 | 0.94 | yes |
| all-uring-1 | ycsb-e | 0.87 | 0.99 | yes |
| all-uring-1 | ycsb-f | 0.98 | 0.94 | yes |
| all-uring-2 | adjacency | 0.61 | 0.54 | yes |
| all-uring-2 | cross-shard-contention | 3.06 | 10.77 | **no** |
| all-uring-2 | group-commit | 0.49 | 0.79 | yes |
| all-uring-2 | skewed-multi-shard | 1.31 | 1.28 | yes |
| all-uring-2 | sparse-wide | 0.65 | 0.45 | yes |
| all-uring-2 | time-series-ttl | 1.05 | 0.96 | yes |
| all-uring-2 | ycsb-a | 0.61 | 0.50 | yes |
| all-uring-2 | ycsb-b | 0.31 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.41 | 1.09 | yes |
| all-uring-2 | ycsb-d | 0.49 | 0.96 | yes |
| all-uring-2 | ycsb-e | 0.87 | 0.99 | yes |
| all-uring-2 | ycsb-f | 1.10 | 1.04 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 816930 | 0.85, 6.11 | 0.10, 0.36 | **no** | 407502 | 0.47, 0.52 | 0.10, 1.38 | yes |
| ycsb-b | 1589315 | 0.38, 0.68 | 0.14, 0.03 | yes | 1269529 | 0.34, 0.44 | 0.13, 0.07 | yes |
| ycsb-c | 2320570 | 0.68, 1.29 | 0.09, 0.16 | yes | 2133481 | 0.67, 1.25 | 0.10, 0.16 | yes |
| ycsb-e | 34487 | 3.84, 5.45 | 1.30, 0.92 | **no** | 28918 | 3.84, 5.09 | 1.44, 0.52 | **no** |
| ycsb-f | 432922 | 1.44, 2.14 | 0.19, 0.14 | **no** | 282564 | 0.60, 0.73 | 0.14, 2.19 | **no** |
| sparse-wide | 71541 | 1.01, 0.80 | 0.57, 0.71 | yes | 66228 | 0.90, 0.86 | 0.51, 0.36 | yes |
| time-series-ttl | 1098535 | 0.34, 0.16 | 0.07, 0.02 | yes | 489358 | 0.50, 0.58 | 0.08, 1.08 | yes |
| cross-shard-contention | 214764 | 2.98, 15.31 | 0.17, 0.49 | **no** | 86455 | 1.25, 1.40 | 0.21, 2.74 | **no** |
| counters | 151583 | 3.58, 8.08 | 0.56, 84.31 | **no** | 103045 | 1.03, 5.74 | 0.26, 42.67 | **no** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 363805 → 551715 | 2.38 → 0.84 µs | 4.9 → 6.0 µs | 74% |
| ycsb-a | 152889 → 158656 | 4.77 → 4.83 µs | 13.7 → 14.7 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 179161 | 4.8 | 14.1 | shard_parks 0.07, shard_wakes 0.07, commit_parks 0.00 |
| shards1 | 153433 | 4.5 | 14.7 | shard_parks 0.00, shard_wakes 0.01, commit_parks 0.00 |
| shards4 | 182335 | 4.6 | 13.9 | shard_parks 0.06, shard_wakes 0.06, commit_parks 0.00 |
| spin100 | 180808 | 4.7 | 14.3 | shard_parks 0.01, shard_wakes 0.01, commit_parks 0.00 |
| spin200 | 166719 | 4.7 | 14.8 | shard_parks 0.00, shard_wakes 0.01, commit_parks 0.00 |
| spin500 | 177475 | 4.6 | 14.2 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 100791 | 6.3 | 28.7 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 0.96 | 1.08 |
| cross-shard-contention | 1.01 | 0.98 |
| group-commit | 0.90 | 1.17 |
| skewed-multi-shard | 0.89 | 1.12 |
| sparse-wide | 0.99 | 1.02 |
| time-series-ttl | 0.95 | 1.10 |
| ycsb-a | 1.04 | 0.98 |
| ycsb-b | 0.99 | 1.00 |
| ycsb-c | 0.99 | 1.01 |
| ycsb-d | 1.02 | 1.00 |
| ycsb-e | 0.98 | 1.02 |
| ycsb-f | 1.20 | 0.96 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3488 |
| pread | 1 | 16 (D222) | 1032 |
| uring | 0 | 16 (D222) | 3143 |
| uring | 1 | 16 (D222) | 1017 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 392794 | 202 | 416 | 227 | 1216 MiB | 1216 MiB | 144 ms | 296 µs |
| 4× budget | 369871 | 107 | 138 | 214 | 1504 MiB | 1504 MiB | 487 ms | 2444 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 175c0ffc7318 |
| build | 2 | 175c0ffc7318 |
| memtable-lookup | 1 | 175c0ffc7318 |
| phase2-gate | 3 | 175c0ffc7318 |
| phase2-gate-2 | 3 | 175c0ffc7318 |
| all-uring-1 | 16 | 175c0ffc7318 |
| all-uring-2 | 16 | 175c0ffc7318 |
| all-pread | 16 | 175c0ffc7318 |
| latency | 5 | 175c0ffc7318 |
| ycsb-a-shards | 3 | 175c0ffc7318 |
| row-cache | 0 | 175c0ffc7318 |
| scaling | 8 | 175c0ffc7318 |
| placement | 2 | 175c0ffc7318 |
| contended | 197 | 175c0ffc7318 |
| open-latency | 0 | 175c0ffc7318 |
| wal-pin | 0 | 175c0ffc7318 |
| cold-get | 39 | 175c0ffc7318 |
| cold-scan | 3 | 175c0ffc7318 |
| scan-cache | 0 | 175c0ffc7318 |
| **total** | **314** | |
