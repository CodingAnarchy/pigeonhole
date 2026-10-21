# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 3ee90c215206f08081f5f6a6b4abc91294add12b; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.83 µs, p99 5.70 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 335 ns (262k entries/miss (lower bound) 262 ns, 1M entries/hit (get) 482 ns, 1M entries/miss (lower bound) 406 ns, 10k entries/hit (get) 128 ns, 10k entries/miss (lower bound) 106 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.050 device reads, 1.046 cache misses per get; p50 87 µs, p99 192 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 10.50M cells/s at 16 shards (656K per core); p50 73 µs, p99 130 µs; busiest shard 6%; 2.0M commits in 0.8 s, 79 flushes, 16 compactions; in the engine p50 66 µs, p99 98 µs, p99.9 786 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 25 µs, p99 96 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 10.00M cells/s (-5% vs pinned); p50 78 µs, p99 157 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.80M cells/s (-54% vs 16); p50 46 µs, p99 79 µs, p99.9 132 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.82M cells/s (-25% vs 16); p50 53 µs, p99 92 µs, p99.9 201 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 11.56M cells/s at 16 shards (722K per core); p50 73 µs, p99 150 µs; 2.0M commits in 0.7 s, 81 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 131 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 4.74M cells/s at 16 shards; p50 156 µs, p99 2638 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 131 µs, p99 2097 µs, p99.9 4194 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 69K cells/s, p99 115 µs, 4 thr: 271K cells/s, p99 142 µs, 16 thr: 781K cells/s, p99 177 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 4.26 GB/s (243738 misses, 30272 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.34 ms (p99 5.22), uring: 2.94 ms (p99 5.21) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 1.00; N=4: 1.11 (busiest 25%; 8.6 s, stalled 0% of shard time, 163 flushes, 43 compactions, max 10.4 ms); N=8: 1.01 (busiest 13%; 8.2 s, stalled 0% of shard time, 287 flushes, 75 compactions, max 46.2 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.65 (busiest 7%; 8.3 s, stalled 0% of shard time, 379 flushes, 91 compactions, max 55.9 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 0.99; N=4: 1.11; N=8: 1.02; N=16: 0.65 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 0.99; N=8: 0.91; N=16: 0.61 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 26.8% (max 31.2%), settled 12 s, 0 changes, 120K of 120K writes/s; N=8: busiest 15.4% (max 15.6%), settled 30 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.7% (max 7.8%), settled 22 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | all within | **pass** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | all-uring-1: 646K ops/s, RocksDB 0.88× throughput, p99 1.43×; tablets on/off 1.01 (packing off); wasted rounds 0.000 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 0, move raise 0, moved 0, room 0, pending overlap 262 (D249); seqno inference 1.000); Busy 0; cross-shard p99 162 µs (≤ 2 ms), 1.52× RocksDB's batch p99; all-uring-2: 661K ops/s, RocksDB 0.86× throughput, p99 1.37×; tablets on/off 0.99 (packing off); wasted rounds 0.000 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 0, move raise 0, moved 0, room 0, pending overlap 227 (D249); seqno inference 1.000); Busy 0; cross-shard p99 158 µs (≤ 2 ms), 1.47× RocksDB's batch p99 | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.67×/p99 5.24×); cross-shard-contention (rocksdb 1.19×/p99 3.02×); counters (rocksdb 3.40×/p99 7.34×, sqlite 0.53×/p99 75.45×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 537K ops/s, RocksDB 1.19× throughput, p99 3.02×; tablets on/off 0.98 (packing off); wasted rounds 0.000 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 0, move raise 0, moved 0, room 0, pending overlap 672 (D249); seqno inference 1.000); Busy 0; cross-shard p99 120 µs (≤ 2 ms), 3.26× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 2.84×/p99 3.57×); ycsb-f (sqlite 0.12×/p99 1.86×); cross-shard-contention (sqlite 0.14×/p99 2.28×); counters (rocksdb 1.26×/p99 8.19×, sqlite 0.32×/p99 59.92×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 131K ops/s, RocksDB 0.83× throughput, p99 1.18×; tablets on/off 1.00 (packing off); wasted rounds 0.000 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 0, move raise 0, moved 0, room 0, pending overlap 260 (D249); seqno inference 1.000); Busy 0; cross-shard p99 266 µs (≤ 1 ms), 1.25× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 113K ops/s, p99 811 µs; rocksdb 0.32× throughput, p99 0.39×, sqlite 0.10× throughput, p99 0.02× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 120K ops/s, p99 901 µs; rocksdb 0.24× throughput, p99 0.10×, sqlite 0.07× throughput, p99 0.03× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 617K ops/s, p50 27 µs, p99 504 µs; 0.51× the synchronous run's throughput (p99 11.31×, queued by design) | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 297K ops/s, p50 167 µs, p99 17170 µs; 0.25× the synchronous run's throughput (p99 385.48×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 913K ops/s, p50 11 µs, p99 367 µs; 0.40× the synchronous run's throughput (p99 13.14×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 923K ops/s, p50 114 µs, p99 696 µs; 0.41× the synchronous run's throughput (p99 24.95×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 507K ops/s, p50 63 µs, p99 545 µs; 1.15× the synchronous run's throughput (p99 5.12×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 487K ops/s, p50 266 µs, p99 1409 µs; 1.11× the synchronous run's throughput (p99 13.23×, queued by design) | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 865K ops/s, p50 12 µs, p99 385 µs; 0.38× the synchronous run's throughput (p99 14.06×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 968K ops/s, p50 110 µs, p99 655 µs; 0.42× the synchronous run's throughput (p99 23.93×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 322K commits/s, p50 47 µs, p99 20578 µs; tablets on 16 of 16 shards after a 30 s warmup, not settled; 18626141 two-phase rounds, 15499337 refused; 0 commits retried after Busy, 3 write stalls (73 ms); 0.07× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 120K commits/s, p50 123 µs, p99 48234 µs; tablets on 16 of 16 shards after a 60 s warmup; 10345589 two-phase rounds, 9173689 refused; 0 commits retried after Busy, 0 write stalls (0 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 328 µs (990 snapshots), durable-snapshot scans p99 311 µs (994); durable backup 30960 ms, compaction 3352 ms; close 733 ms; foreground p99 64 µs, 1.44× the judged ycsb-a's | - | `contended-buffered-ycsb-a-riders.json` |
| Contended ycsb-a with riders, default durability, GroupSync (reported, not judged; D247) | snapshot scans p99 274 µs (553 snapshots), durable-snapshot scans p99 272 µs (554); durable backup 38955 ms, compaction 1710 ms; close 598 ms; foreground p99 129 µs, 1.21× the judged ycsb-a's | - | `contended-default-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 5144 ops/s, p99 48759 µs; 1.140 conflicts per transfer, 0.971 pending-share overlaps retried in the engine (D249); rocksdb 99.28× throughput, p99 151.64×, sqlite 8.16× throughput, p99 155.61×; **> 3× worse than cross-shard-contention against RocksDB (99.28× vs 1.19×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 97K ops/s, p99 1499 µs; rocksdb 0.34× throughput, p99 0.43×, sqlite 0.90× throughput, p99 0.47× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 171K ops/s, p99 1360 µs; rocksdb 0.32× throughput, p99 0.51×, sqlite 0.14× throughput, p99 0.28× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 27K ops/s, p99 5800 µs; rocksdb 0.92× throughput, p99 0.42×, sqlite 0.41× throughput, p99 0.20× | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 5624 ops/s, p99 41943 µs; 1.000 conflicts per transfer, 1.300 pending-share overlaps retried in the engine (D249); rocksdb 9.61× throughput, p99 12.86×, sqlite 3.37× throughput, p99 411.66×; **> 3× worse than cross-shard-contention against RocksDB (9.61× vs 0.83×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 75K ops/s, p99 1982 µs; rocksdb 0.49× throughput, p99 0.56×, sqlite 0.71× throughput, p99 0.61× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 112K ops/s, p99 1614 µs; rocksdb 0.44× throughput, p99 0.59×, sqlite 0.23× throughput, p99 0.36× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 17K ops/s, p99 7143 µs; rocksdb 1.25× throughput, p99 0.54×, sqlite 0.73× throughput, p99 0.38× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 129 s); buffered-adjacency-prefix (rocksdb, 132 s); buffered-adjacency-prefix (sqlite-eav, 134 s); buffered-sparse-wide (rocksdb, 170 s); buffered-time-series-range (pigeonhole, 134 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 159 s); default-adjacency-prefix (pigeonhole, 131 s); default-adjacency-prefix (rocksdb, 132 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 1 with different background work (D233) | **pass** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.07×, scan 1.00×; run 2: row read 1.40×, scan 1.21× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.57 | 0.50 | yes |
| all-uring-1 | cross-shard-contention | 0.88 | 1.43 | yes |
| all-uring-1 | group-commit | 0.42 | 0.70 | yes |
| all-uring-1 | skewed-multi-shard | 0.86 | 0.89 | yes |
| all-uring-1 | sparse-wide | 0.68 | 0.44 | yes |
| all-uring-1 | time-series-ttl | 0.89 | 0.62 | yes |
| all-uring-1 | ycsb-a | 0.57 | 0.51 | yes |
| all-uring-1 | ycsb-b | 0.32 | 0.58 | yes |
| all-uring-1 | ycsb-c | 0.42 | 1.12 | yes |
| all-uring-1 | ycsb-d | 0.41 | 0.93 | yes |
| all-uring-1 | ycsb-e | 0.81 | 0.98 | yes |
| all-uring-1 | ycsb-f | 0.97 | 0.94 | yes |
| all-uring-2 | adjacency | 0.57 | 0.51 | yes |
| all-uring-2 | cross-shard-contention | 0.86 | 1.37 | yes |
| all-uring-2 | group-commit | 0.43 | 0.74 | yes |
| all-uring-2 | skewed-multi-shard | 0.91 | 0.91 | yes |
| all-uring-2 | sparse-wide | 0.69 | 0.46 | yes |
| all-uring-2 | time-series-ttl | 0.90 | 0.69 | yes |
| all-uring-2 | ycsb-a | 0.57 | 0.52 | yes |
| all-uring-2 | ycsb-b | 0.32 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.41 | 1.08 | yes |
| all-uring-2 | ycsb-d | 0.42 | 0.99 | yes |
| all-uring-2 | ycsb-e | 0.82 | 0.93 | yes |
| all-uring-2 | ycsb-f | 1.12 | 0.92 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 1203227 | 0.58, 0.65 | 0.07, 0.04 | yes | 439746 | 0.43, 0.51 | 0.09, 1.34 | yes |
| ycsb-b | 1652015 | 0.36, 0.66 | 0.14, 0.03 | yes | 1285040 | 0.34, 0.43 | 0.13, 0.07 | yes |
| ycsb-c | 2266508 | 0.62, 1.20 | 0.09, 0.15 | yes | 2302109 | 0.70, 1.27 | 0.09, 0.16 | yes |
| ycsb-e | 35430 | 3.67, 5.24 | 1.29, 0.88 | **no** | 37346 | 2.84, 3.57 | 1.14, 0.39 | **no** |
| ycsb-f | 556492 | 1.15, 1.20 | 0.15, 0.08 | yes | 313710 | 0.55, 0.63 | 0.12, 1.86 | **no** |
| sparse-wide | 78859 | 0.97, 0.78 | 0.53, 0.67 | yes | 72826 | 0.82, 0.74 | 0.47, 0.31 | yes |
| time-series-ttl | 1194629 | 0.31, 0.15 | 0.06, 0.02 | yes | 527997 | 0.47, 0.56 | 0.08, 1.07 | yes |
| cross-shard-contention | 536544 | 1.19, 3.02 | 0.07, 0.10 | **no** | 130580 | 0.83, 1.18 | 0.14, 2.28 | **no** |
| counters | 159582 | 3.40, 7.34 | 0.53, 75.45 | **no** | 84176 | 1.26, 8.19 | 0.32, 59.92 | **no** |

The judged p99 above is over every operation. Each operation type's p99 against the same type in each store (reported, not judged; D261; > 1.5× in bold):

| Workload | Level | Operation | Pigeonhole p99 µs | vs RocksDB p99 | vs SQLite p99 |
|---|---|---|--:|--:|--:|
| ycsb-a | buffered | put | 16.8 | 0.84 | 0.00 |
| ycsb-a | buffered | row read | 50.2 | 0.67 | **1.65** |
| ycsb-a | default | put | 123.4 | 0.56 | 0.61 |
| ycsb-a | default | row read | 38.9 | 0.55 | **2.19** |
| ycsb-b | buffered | put | 16.8 | 1.18 | 0.00 |
| ycsb-b | buffered | row read | 35.6 | 0.66 | 0.38 |
| ycsb-b | default | put | 128.0 | 0.13 | 0.00 |
| ycsb-b | default | row read | 36.6 | 0.53 | 0.92 |
| ycsb-c | buffered | row read | 27.9 | 1.20 | 0.15 |
| ycsb-c | default | row read | 27.4 | 1.27 | 0.16 |
| ycsb-e | buffered | put | 82.9 | **3.60** | 0.00 |
| ycsb-e | buffered | scan | 1417.2 | **5.20** | **1.99** |
| ycsb-e | default | put | 185.3 | 0.06 | 0.00 |
| ycsb-e | default | scan | 1351.7 | **3.86** | **2.26** |
| ycsb-f | buffered | read-modify-write | 94.2 | **2.31** | 0.03 |
| ycsb-f | buffered | row read | 56.8 | 0.74 | **1.91** |
| ycsb-f | default | read-modify-write | 163.8 | 0.58 | 0.96 |
| ycsb-f | default | row read | 44.0 | 0.61 | **2.44** |
| sparse-wide | buffered | get | 31.1 | 1.03 | 0.42 |
| sparse-wide | buffered | put | 26.4 | **1.56** | 0.00 |
| sparse-wide | buffered | row read | 2850.8 | 0.77 | 1.20 |
| sparse-wide | buffered | scan | 2916.4 | 0.76 | 1.11 |
| sparse-wide | default | get | 31.1 | 0.87 | 0.63 |
| sparse-wide | default | put | 1196.0 | 0.47 | 0.04 |
| sparse-wide | default | row read | 3113.0 | 0.73 | **2.38** |
| sparse-wide | default | scan | 3162.1 | 0.71 | **2.14** |
| time-series-ttl | buffered | get | 13.1 | 0.58 | 0.51 |
| time-series-ttl | buffered | put at | 24.3 | 1.23 | 0.00 |
| time-series-ttl | buffered | scan | 56.8 | 0.16 | **2.07** |
| time-series-ttl | default | get | 7.5 | 0.73 | 0.45 |
| time-series-ttl | default | put at | 142.3 | 0.64 | 0.05 |
| time-series-ttl | default | scan | 24.6 | 0.42 | 1.34 |
| cross-shard-contention | buffered | cross-shard commit | 120.3 | **3.26** | 0.10 |
| cross-shard-contention | buffered | put | 84.0 | **2.47** | 0.25 |
| cross-shard-contention | default | cross-shard commit | 266.2 | 1.25 | **2.15** |
| cross-shard-contention | default | put | 150.5 | 0.73 | **1.78** |
| counters | buffered | counter read | 11272.2 | **7.73** | **876.33** |
| counters | buffered | incr | 16.1 | 0.51 | 0.34 |
| counters | default | counter read | 13893.6 | **7.96** | **1608.25** |
| counters | default | incr | 491.5 | **2.38** | **6.71** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 371148 → 575876 | 2.48 → 0.83 µs | 5.0 → 5.7 µs | 74% |
| ycsb-a | 172713 → 182289 | 4.80 → 4.67 µs | 12.9 → 14.5 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 179388 | 5.0 | 14.3 | shard_parks 0.02, shard_wakes 0.02, commit_parks 0.00 |
| shards1 | 193575 | 4.5 | 14.1 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 173370 | 4.8 | 15.4 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin0 | 134203 | 8.3 | 14.4 | shard_parks 0.50, shard_wakes 0.50, commit_parks 0.00 |
| spin100 | 172554 | 5.0 | 15.0 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 182578 | 4.8 | 14.1 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 100762 | 6.3 | 28.8 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.01 | 0.97 |
| cross-shard-contention | 0.97 | 1.00 |
| group-commit | 1.14 | 0.92 |
| skewed-multi-shard | 1.01 | 0.98 |
| sparse-wide | 1.06 | 0.95 |
| time-series-ttl | 1.04 | 0.89 |
| ycsb-a | 1.00 | 1.00 |
| ycsb-b | 0.95 | 1.01 |
| ycsb-c | 0.99 | 1.03 |
| ycsb-d | 1.05 | 0.94 |
| ycsb-e | 0.99 | 1.16 |
| ycsb-f | 1.02 | 1.02 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3438 |
| pread | 1 | 16 (D222) | 1033 |
| uring | 0 | 16 (D222) | 3071 |
| uring | 1 | 16 (D222) | 1015 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 447119 | 190 | 418 | 225 | 1216 MiB | 1216 MiB | 342 ms | 114 µs |
| 4× budget | 423155 | 108 | 147 | 220 | 1472 MiB | 1472 MiB | 669 ms | 290 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 3ee90c215206 |
| build | 0 | 3ee90c215206 |
| memtable-lookup | 1 | 3ee90c215206 |
| phase2-gate | 3 | 3ee90c215206 |
| phase2-gate-2 | 3 | 3ee90c215206 |
| all-uring-1 | 17 | 3ee90c215206 |
| all-uring-2 | 17 | 3ee90c215206 |
| all-pread | 17 | 3ee90c215206 |
| latency | 5 | 3ee90c215206 |
| ycsb-a-shards | 3 | 3ee90c215206 |
| row-cache | 1 | 3ee90c215206 |
| scaling | 8 | 3ee90c215206 |
| placement | 1 | 3ee90c215206 |
| contended | 207 | 3ee90c215206 |
| open-latency | 0 | 3ee90c215206 |
| wal-pin | 0 | 3ee90c215206 |
| cold-get | 36 | 3ee90c215206 |
| cold-scan | 2 | 3ee90c215206 |
| scan-cache | 0 | 3ee90c215206 |
| **total** | **321** | |
