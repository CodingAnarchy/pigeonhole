# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 5933e23e6f4fa0ce70bfa9e4ccd34a6879b517dc; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.80 µs, p99 5.79 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 337 ns (262k entries/miss (lower bound) 261 ns, 1M entries/hit (get) 481 ns, 1M entries/miss (lower bound) 399 ns, 10k entries/hit (get) 128 ns, 10k entries/miss (lower bound) 105 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.050 device reads, 1.046 cache misses per get; p50 87 µs, p99 190 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 10.38M cells/s at 16 shards (649K per core); p50 74 µs, p99 132 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 98 µs, p99.9 786 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 25 µs, p99 80 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 10.28M cells/s (-1% vs pinned); p50 77 µs, p99 159 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.73M cells/s (-54% vs 16); p50 47 µs, p99 79 µs, p99.9 125 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.66M cells/s (-26% vs 16); p50 53 µs, p99 92 µs, p99.9 166 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 11.46M cells/s at 16 shards (716K per core); p50 73 µs, p99 151 µs; 2.0M commits in 0.7 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 131 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 4.75M cells/s at 16 shards; p50 158 µs, p99 2015 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 131 µs, p99 1573 µs, p99.9 4194 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 69K cells/s, p99 113 µs, 4 thr: 270K cells/s, p99 143 µs, 16 thr: 701K cells/s, p99 178 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.43 GB/s (0 misses, 270571 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.42 ms (p99 5.68), uring: 2.92 ms (p99 4.83) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 0.94; N=4: 1.11 (busiest 25%; 8.6 s, stalled 0% of shard time, 163 flushes, 42 compactions, max 9.1 ms); N=8: 1.02 (busiest 13%; 8.2 s, stalled 0% of shard time, 287 flushes, 69 compactions, max 55.8 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.64 (busiest 7%; 8.3 s, stalled 0% of shard time, 377 flushes, 91 compactions, max 56.8 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 0.95; N=4: 1.11; N=8: 1.03; N=16: 0.65 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.00; N=8: 0.89; N=16: 0.64 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 29.9% (max 31.2%), settled 21 s, 0 changes, 120K of 120K writes/s; N=8: busiest 14.1% (max 15.6%), settled 16 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.6% (max 7.8%), settled 22 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | outside: cross-shard-contention (all-uring-1), cross-shard-contention (all-uring-2) | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | all-uring-1: 399K ops/s, RocksDB 1.45× throughput, p99 1.71×; tablets on/off 1.01 (packing off); wasted rounds 0.011 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 26225, move raise 0, moved 45, room 0, pending overlap 0 (D249); seqno inference 0.011); Busy 0; cross-shard p99 174 µs (≤ 2 ms), 1.70× RocksDB's batch p99; all-uring-2: 399K ops/s, RocksDB 1.45× throughput, p99 1.62×; tablets on/off 0.99 (packing off); wasted rounds 0.011 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 24550, move raise 0, moved 26, room 0, pending overlap 0 (D249); seqno inference 0.011); Busy 0; cross-shard p99 171 µs (≤ 2 ms), 1.62× RocksDB's batch p99 | **FAIL** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.91×/p99 5.68×); cross-shard-contention (rocksdb 1.72×/p99 3.77×); counters (rocksdb 3.18×/p99 7.58×, sqlite 0.50×/p99 78.37×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 369K ops/s, RocksDB 1.72× throughput, p99 3.77×; tablets on/off 0.95 (packing off); wasted rounds 0.010 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 185398, move raise 0, moved 25, room 0, pending overlap 0 (D249); seqno inference 0.010); Busy 0; cross-shard p99 142 µs (≤ 2 ms), 3.76× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.07×/p99 3.93×); ycsb-f (sqlite 0.13×/p99 2.13×); cross-shard-contention (sqlite 0.19×/p99 2.45×); counters (rocksdb 1.30×/p99 7.32×, sqlite 0.33×/p99 53.56×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 97K ops/s, RocksDB 1.12× throughput, p99 1.28×; tablets on/off 0.98 (packing off); wasted rounds 0.004 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 12596, move raise 0, moved 154, room 0, pending overlap 0 (D249); seqno inference 0.004); Busy 0; cross-shard p99 276 µs (≤ 1 ms), 1.32× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 112K ops/s, p99 651 µs; rocksdb 0.33× throughput, p99 0.32×, sqlite 0.10× throughput, p99 0.02× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 123K ops/s, p99 958 µs; rocksdb 0.24× throughput, p99 0.10×, sqlite 0.07× throughput, p99 0.03× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 686K ops/s, p50 24 µs, p99 430 µs; 0.60× the synchronous run's throughput (p99 9.13×, queued by design) | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 71K ops/s, p50 38 µs, p99 20185 µs; 0.06× the synchronous run's throughput (p99 428.53×, queued by design); **under a third of the synchronous run's throughput: file an issue** | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 926K ops/s, p50 12 µs, p99 356 µs; 0.40× the synchronous run's throughput (p99 12.77×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 934K ops/s, p50 113 µs, p99 684 µs; 0.41× the synchronous run's throughput (p99 24.51×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 563K ops/s, p50 61 µs, p99 459 µs; 1.29× the synchronous run's throughput (p99 4.37×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 572K ops/s, p50 222 µs, p99 1036 µs; 1.31× the synchronous run's throughput (p99 9.87×, queued by design) | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 899K ops/s, p50 12 µs, p99 371 µs; 0.42× the synchronous run's throughput (p99 13.16×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 946K ops/s, p50 112 µs, p99 680 µs; 0.44× the synchronous run's throughput (p99 24.15×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 825K commits/s, p50 168 µs, p99 1294 µs; tablets on 16 of 16 shards after a 21 s warmup; 7574990 two-phase rounds, 147587 refused; 0 commits retried after Busy, 4152 write stalls (6666 ms); 0.19× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 368K commits/s, p50 332 µs, p99 4178 µs; tablets on 16 of 16 shards after a 46 s warmup; 3563308 two-phase rounds, 44558 refused; 0 commits retried after Busy, 0 write stalls (0 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 305 µs (918 snapshots), durable-snapshot scans p99 307 µs (911); durable backup 27831 ms, compaction 1649 ms; close 632 ms; foreground p99 73 µs, 1.55× the judged ycsb-a's | - | `contended-buffered-ycsb-a-riders.json` |
| Contended ycsb-a with riders, default durability, GroupSync (reported, not judged; D247) | snapshot scans p99 274 µs (574 snapshots), durable-snapshot scans p99 279 µs (579); durable backup 32217 ms, compaction 2036 ms; close 503 ms; foreground p99 138 µs, 1.32× the judged ycsb-a's | - | `contended-default-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 14K ops/s, p99 14549 µs; 1.114 conflicts per transfer, 0.515 pending-share overlaps retried in the engine (D249); rocksdb 36.94× throughput, p99 45.54×, sqlite 3.03× throughput, p99 42.80×; **> 3× worse than cross-shard-contention against RocksDB (36.94× vs 1.72×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 88K ops/s, p99 1827 µs; rocksdb 0.38× throughput, p99 0.54×, sqlite 0.97× throughput, p99 0.58× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 166K ops/s, p99 1360 µs; rocksdb 0.33× throughput, p99 0.51×, sqlite 0.15× throughput, p99 0.27× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 17K ops/s, p99 5931 µs; rocksdb 1.49× throughput, p99 0.44×, sqlite 0.68× throughput, p99 0.21×; **> 3× worse than time-series-ttl against RocksDB (1.49× vs 0.30×): file an issue** | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 6554 ops/s, p99 24904 µs; 10.771 conflicts per transfer, 0.265 pending-share overlaps retried in the engine (D249); rocksdb 8.25× throughput, p99 7.68×, sqlite 2.97× throughput, p99 240.79×; **> 3× worse than cross-shard-contention against RocksDB (8.25× vs 1.12×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 70K ops/s, p99 2089 µs; rocksdb 0.54× throughput, p99 0.60×, sqlite 0.77× throughput, p99 0.65× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 110K ops/s, p99 1638 µs; rocksdb 0.46× throughput, p99 0.60×, sqlite 0.24× throughput, p99 0.37× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 19K ops/s, p99 7373 µs; rocksdb 1.11× throughput, p99 0.55×, sqlite 0.66× throughput, p99 0.40× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 128 s); buffered-adjacency-prefix (rocksdb, 132 s); buffered-adjacency-prefix (sqlite-eav, 134 s); buffered-sparse-wide (rocksdb, 170 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 157 s); default-adjacency-prefix (pigeonhole, 131 s); default-adjacency-prefix (rocksdb, 133 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (pigeonhole, 134 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 1 metrics outside tolerance; 1 with different background work (D233) | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.32×, scan 1.17×; run 2: row read 0.94×, scan 1.03× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.59 | 0.52 | yes |
| all-uring-1 | cross-shard-contention | 1.45 | 1.71 | **no** |
| all-uring-1 | group-commit | 0.44 | 0.71 | yes |
| all-uring-1 | skewed-multi-shard | 1.01 | 1.10 | yes |
| all-uring-1 | sparse-wide | 0.71 | 0.46 | yes |
| all-uring-1 | time-series-ttl | 0.98 | 0.80 | yes |
| all-uring-1 | ycsb-a | 0.62 | 0.50 | yes |
| all-uring-1 | ycsb-b | 0.31 | 0.58 | yes |
| all-uring-1 | ycsb-c | 0.41 | 1.10 | yes |
| all-uring-1 | ycsb-d | 0.44 | 0.97 | yes |
| all-uring-1 | ycsb-e | 0.81 | 0.83 | yes |
| all-uring-1 | ycsb-f | 0.90 | 0.85 | yes |
| all-uring-2 | adjacency | 0.59 | 0.52 | yes |
| all-uring-2 | cross-shard-contention | 1.45 | 1.62 | **no** |
| all-uring-2 | group-commit | 0.49 | 0.82 | yes |
| all-uring-2 | skewed-multi-shard | 0.99 | 1.01 | yes |
| all-uring-2 | sparse-wide | 0.70 | 0.45 | yes |
| all-uring-2 | time-series-ttl | 0.97 | 0.82 | yes |
| all-uring-2 | ycsb-a | 0.58 | 0.52 | yes |
| all-uring-2 | ycsb-b | 0.32 | 0.55 | yes |
| all-uring-2 | ycsb-c | 0.41 | 1.10 | yes |
| all-uring-2 | ycsb-d | 0.43 | 0.95 | yes |
| all-uring-2 | ycsb-e | 0.77 | 0.74 | yes |
| all-uring-2 | ycsb-f | 1.11 | 0.96 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 1139331 | 0.61, 0.69 | 0.07, 0.04 | yes | 435229 | 0.44, 0.50 | 0.09, 1.31 | yes |
| ycsb-b | 1557198 | 0.39, 0.70 | 0.14, 0.03 | yes | 1323399 | 0.33, 0.45 | 0.13, 0.07 | yes |
| ycsb-c | 2291490 | 0.70, 1.31 | 0.09, 0.16 | yes | 2167300 | 0.66, 1.23 | 0.10, 0.16 | yes |
| ycsb-e | 33653 | 3.91, 5.68 | 1.35, 0.95 | **no** | 35824 | 3.07, 3.93 | 1.18, 0.41 | **no** |
| ycsb-f | 546301 | 1.16, 1.14 | 0.15, 0.07 | yes | 305366 | 0.56, 0.72 | 0.13, 2.13 | **no** |
| sparse-wide | 81637 | 0.94, 0.75 | 0.51, 0.63 | yes | 71822 | 0.82, 0.67 | 0.47, 0.29 | yes |
| time-series-ttl | 1270119 | 0.30, 0.13 | 0.06, 0.01 | yes | 532900 | 0.46, 0.57 | 0.08, 1.06 | yes |
| cross-shard-contention | 369463 | 1.72, 3.77 | 0.10, 0.12 | **no** | 97190 | 1.12, 1.28 | 0.19, 2.45 | **no** |
| counters | 168683 | 3.18, 7.58 | 0.50, 78.37 | **no** | 81872 | 1.30, 7.32 | 0.33, 53.56 | **no** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 385219 → 578282 | 2.30 → 0.80 µs | 4.8 → 5.8 µs | 74% |
| ycsb-a | 155977 → 177379 | 5.34 → 4.77 µs | 13.8 → 13.9 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 163130 | 5.1 | 15.2 | shard_parks 0.08, shard_wakes 0.08, commit_parks 0.00 |
| shards1 | 187862 | 4.6 | 14.1 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 183090 | 4.6 | 14.2 | shard_parks 0.01, shard_wakes 0.01, commit_parks 0.00 |
| spin100 | 178699 | 4.8 | 14.5 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin200 | 185081 | 4.6 | 14.3 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 176249 | 4.7 | 15.1 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 101839 | 6.1 | 28.7 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 0.99 | 1.01 |
| cross-shard-contention | 1.01 | 1.00 |
| group-commit | 1.03 | 1.06 |
| skewed-multi-shard | 0.94 | 1.11 |
| sparse-wide | 1.05 | 1.01 |
| time-series-ttl | 0.97 | 0.98 |
| ycsb-a | 0.97 | 0.99 |
| ycsb-b | 1.02 | 0.95 |
| ycsb-c | 1.00 | 1.01 |
| ycsb-d | 0.98 | 1.01 |
| ycsb-e | 1.06 | 0.91 |
| ycsb-f | 1.43 | 0.82 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3390 |
| pread | 1 | 16 (D222) | 1028 |
| uring | 0 | 16 (D222) | 3097 |
| uring | 1 | 16 (D222) | 1018 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 428176 | 204 | 424 | 228 | 1216 MiB | 1216 MiB | 157 ms | 246 µs |
| 4× budget | 400562 | 106 | 128 | 214 | 1472 MiB | 1472 MiB | 758 ms | 164 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 5933e23e6f4f |
| build | 2 | 5933e23e6f4f |
| memtable-lookup | 1 | 5933e23e6f4f |
| phase2-gate | 3 | 5933e23e6f4f |
| phase2-gate-2 | 3 | 5933e23e6f4f |
| all-uring-1 | 17 | 5933e23e6f4f |
| all-uring-2 | 18 | 5933e23e6f4f |
| all-pread | 17 | 5933e23e6f4f |
| latency | 5 | 5933e23e6f4f |
| ycsb-a-shards | 3 | 5933e23e6f4f |
| row-cache | 1 | 5933e23e6f4f |
| scaling | 8 | 5933e23e6f4f |
| placement | 1 | 5933e23e6f4f |
| contended | 199 | 5933e23e6f4f |
| open-latency | 0 | 5933e23e6f4f |
| wal-pin | 0 | 5933e23e6f4f |
| cold-get | 37 | 5933e23e6f4f |
| cold-scan | 2 | 5933e23e6f4f |
| scan-cache | 0 | 5933e23e6f4f |
| **total** | **317** | |
