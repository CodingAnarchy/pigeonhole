# Phase 3 gate window: summary

- **Machine:** AMD Ryzen 9 7950X3D 16-Core Processor, 32 CPUs, MemTotal:       131361660 kB
- **Kernel:** 6.8.0-139-generic; **drive:** mn        : SAMSUNG MZQL21T9HCJR-00A07              
fr        : GDC5A02Q
vwc       : 0x6; **mount:** xfs    rw,noatime,attr2,inode64,logbufs=8,logbsize=32k,sunit=256,swidth=256,noquota
- **Commit:** 516b59b8f230ce29fd33425ddf6ae94e8dbc5d1e; **scale:** full
- **CCD (D238):** 8 physical cores per L3 (L3s: 8, 8)
- **Warning:** nvme0n1 reports a volatile write cache (vwci      : 0
vwc       : 0x6): check it has power-loss protection

## Targets (#406)

| Target | Measured | Verdict | Source |
|---|---|---|---|
| Point get in memory: p50 < 2 µs, p99 < 10 µs | p50 0.80 µs, p99 5.98 µs | **pass** | `latency-ycsb-c.json` |
| Memtable point lookup at 2^18 entries (D230): ≤ 300 ns (#17); 337 ns accepted (D239) | 262k hit 338 ns (262k entries/miss (lower bound) 265 ns, 1M entries/hit (get) 483 ns, 1M entries/miss (lower bound) 402 ns, 10k entries/hit (get) 129 ns, 10k entries/miss (lower bound) 107 ns) | accepted (D239) | `memtable-lookup.txt` |
| Point get on cold data: one I/O | 1.050 device reads, 1.046 cache misses per get; p50 87 µs, p99 192 µs; block cache 1024 MiB | **pass** | `cold-get.jsonl` |
| Durable group commit: > 1M cells/s across cores; p99 < 200 µs (D213, D226: 16 in flight) | 10.07M cells/s at 16 shards (630K per core); p50 74 µs, p99 133 µs; busiest shard 6%; 2.0M commits in 0.8 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 98 µs, p99.9 786 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0; disk fdatasync p50 24 µs, p99 84 µs | **pass** | `latency-group-commit.json` |
| Durable group commit, shard threads unpinned (reported, not judged; D235) | 9.71M cells/s (-4% vs pinned); p50 79 µs, p99 167 µs | - | `latency-group-commit.json` |
| Durable group commit at 4 in flight per shard (reported, not judged; D226) | 4.80M cells/s (-52% vs 16); p50 46 µs, p99 79 µs, p99.9 128 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-4.json` |
| Durable group commit at 8 in flight per shard (reported, not judged; D226) | 7.52M cells/s (-25% vs 16); p50 53 µs, p99 93 µs, p99.9 229 µs; in the engine p99 66 µs | - | `latency-group-commit-inflight-8.json` |
| Durable group commit on pread (reported, not judged; D213: decides the io_uring default) | 11.67M cells/s at 16 shards (730K per core); p50 73 µs, p99 151 µs; 2.0M commits in 0.7 s, 80 flushes, 16 compactions; in the engine p50 66 µs, p99 131 µs, p99.9 131 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-pread.json` |
| Durable group commit, short run (reported, not judged; D219: the first window's length) | 4.69M cells/s at 16 shards; p50 159 µs, p99 1417 µs; 0.2M commits in 0.2 s, 16 flushes, 0 compactions; in the engine p50 131 µs, p99 1049 µs, p99.9 3146 µs; WAL rollover blocks 0, inline syncs 0; tablet changes while measuring 0 | - | `latency-group-commit-short.json` |
| Durable group commit, synchronous clients (reported, not judged; D213) | 1 thr: 70K cells/s, p99 115 µs, 4 thr: 283K cells/s, p99 141 µs, 16 thr: 777K cells/s, p99 176 µs | - | `latency-group-commit.json` |
| Ordered row scan from cache: > 1 GB/s per core | 6.75 GB/s (0 misses, 270566 hits) | **pass** | `scan-cache.jsonl` |
| Open to first read: < 5 ms | pread: 3.41 ms (p99 5.72), uring: 2.94 ms (p99 4.68) | **pass** | `open-latency.jsonl` |
| Thread-per-core scaling: ≥ 0.8 × N for N up to one CCD (D238; one CCD is 8 physical cores, from the L3 topology) | N=1: 1.00; N=4: 1.16 (busiest 25%; 8.3 s, stalled 0% of shard time, 163 flushes, 43 compactions, max 10.3 ms); N=8: 1.02 (busiest 13%; 8.1 s, stalled 0% of shard time, 287 flushes, 75 compactions, max 44.0 ms) | **pass** | `scaling-N.json` |
| Thread-per-core scaling above one CCD (reported, not judged; D238: ~0.70 accepted on two-chiplet parts) | N=16: 0.65 (busiest 7%; 8.3 s, stalled 0% of shard time, 379 flushes, 91 compactions, max 54.3 ms) | - | `scaling-N.json` |
| Thread-per-core scaling against a fixed single-shard rate, 417,922 commits/s (reported, not judged; D238) | N=1: 1.00; N=4: 1.16; N=8: 1.03; N=16: 0.65 | - | `scaling-N.json` |
| Thread-per-core scaling, shard threads unpinned (reported, not judged; D235) | N=4: 1.03; N=8: 0.92; N=16: 0.60 | - | `scaling-N.json` |
| Balancer placement: busiest ≤ 1.25/N once settled (D231) | N=4: busiest 29.3% (max 31.2%), settled 21 s, 0 changes, 120K of 120K writes/s; N=8: busiest 15.1% (max 15.6%), settled 13 s, 0 changes, 240K of 240K writes/s; N=16: busiest 7.5% (max 7.8%), settled 19 s, 0 changes, 480K of 480K writes/s | **pass** | `placement-N.json` |
| Within 1.5× of RocksDB on every workload | all within | **pass** | `all-uring-1.json, all-uring-2.json` |
| Contended cross-shard commits (D246): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | all-uring-1: 593K ops/s, RocksDB 0.96× throughput, p99 1.33×; tablets on/off 1.01 (packing off); wasted rounds 0.013 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 49028, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.013); Busy 0; cross-shard p99 137 µs (≤ 2 ms), 1.32× RocksDB's batch p99; all-uring-2: 584K ops/s, RocksDB 0.97× throughput, p99 1.28×; tablets on/off 1.00 (packing off); wasted rounds 0.012 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 45322, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.012); Busy 0; cross-shard p99 137 µs (≤ 2 ms), 1.28× RocksDB's batch p99 | **pass** | `all-uring-1.json, all-uring-2.json` |
| Contended, 16 threads on the default options, D108 parity, buffered (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.90×/p99 5.76×); cross-shard-contention (rocksdb 1.34×/p99 2.95×); counters (rocksdb 3.42×/p99 7.62×, sqlite 0.54×/p99 82.16×) | **FAIL** | `contended-buffered-*.json` |
| Contended cross-shard commits at 16 threads, D108 parity, buffered (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 475K ops/s, RocksDB 1.34× throughput, p99 2.95×; tablets on/off 0.97 (packing off); wasted rounds 0.010 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 156357, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.010); Busy 0; cross-shard p99 113 µs (≤ 2 ms), 2.95× RocksDB's batch p99 | **FAIL** | `contended-buffered-cross-shard-contention.json` |
| Contended, 16 threads on the default options, default durability, GroupSync (D247): within 1.5× of RocksDB and SQLite | outside: ycsb-e (rocksdb 3.27×/p99 4.24×); ycsb-f (sqlite 0.12×/p99 2.04×); cross-shard-contention (sqlite 0.17×/p99 2.29×); counters (rocksdb 1.30×/p99 7.12×, sqlite 0.33×/p99 51.52×) | **FAIL** | `contended-default-*.json` |
| Contended cross-shard commits at 16 threads, default durability, GroupSync (D246, D247): within 1.5× of RocksDB; tablets on ≥ 0.8× off (packing off); ≤ 0.1 wasted 2PC rounds per cross-shard commit; no Busy; cross-shard p99 ≤ 1.5× RocksDB's batch p99, ≤ 2 ms at Buffered and ≤ 1 ms at GroupSync | 107K ops/s, RocksDB 1.03× throughput, p99 1.18×; tablets on/off 1.00 (packing off); wasted rounds 0.004 per cross-shard commit (refused rounds, ICR 0043; refusals: below floor 13064, move raise 0, moved 0, room 0, pending overlap 0 (D249); seqno inference 0.004); Busy 0; cross-shard p99 255 µs (≤ 1 ms), 1.20× RocksDB's batch p99 | **pass** | `contended-default-cross-shard-contention.json` |
| Contended ycsb-a, 16 KiB values, D108 parity, buffered (reported, not judged; D247) | 113K ops/s, p99 741 µs; rocksdb 0.32× throughput, p99 0.36×, sqlite 0.10× throughput, p99 0.02× | - | `contended-buffered-ycsb-a-16k.json` |
| Contended ycsb-a, 16 KiB values, default durability, GroupSync (reported, not judged; D247) | 119K ops/s, p99 913 µs; rocksdb 0.25× throughput, p99 0.10×, sqlite 0.07× throughput, p99 0.03× | - | `contended-default-ycsb-a-16k.json` |
| Contended ycsb-a through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 673K ops/s, p50 25 µs, p99 453 µs; 0.58× the synchronous run's throughput (p99 8.97×, queued by design) | - | `contended-buffered-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 473K ops/s, p50 144 µs, p99 11141 µs; 0.41× the synchronous run's throughput (p99 220.92×, queued by design) | - | `contended-buffered-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, D108 parity, buffered (reported, not judged; D247) | 871K ops/s, p50 12 µs, p99 385 µs; 0.38× the synchronous run's throughput (p99 13.67×, queued by design) | - | `contended-buffered-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, D108 parity, buffered (reported, not judged; D247) | 947K ops/s, p50 111 µs, p99 676 µs; 0.41× the synchronous run's throughput (p99 24.00×, queued by design) | - | `contended-buffered-ycsb-c-async16.json` |
| Contended ycsb-a through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 491K ops/s, p50 64 µs, p99 573 µs; 1.11× the synchronous run's throughput (p99 5.46×, queued by design) | - | `contended-default-ycsb-a-async4.json` |
| Contended ycsb-a through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 547K ops/s, p50 225 µs, p99 1245 µs; 1.23× the synchronous run's throughput (p99 11.86×, queued by design) | - | `contended-default-ycsb-a-async16.json` |
| Contended ycsb-c through the async API, 4 in flight per client, default durability, GroupSync (reported, not judged; D247) | 882K ops/s, p50 12 µs, p99 381 µs; 0.38× the synchronous run's throughput (p99 13.84×, queued by design) | - | `contended-default-ycsb-c-async4.json` |
| Contended ycsb-c through the async API, 16 in flight per client, default durability, GroupSync (reported, not judged; D247) | 949K ops/s, p50 112 µs, p99 672 µs; 0.41× the synchronous run's throughput (p99 24.41×, queued by design) | - | `contended-default-ycsb-c-async16.json` |
| Application-owned mode on the defaults, D108 parity, buffered (reported, not judged; D247) | 886K commits/s, p50 168 µs, p99 512 µs; tablets on 16 of 16 shards after a 15 s warmup; 7964358 two-phase rounds, 146358 refused; 0 commits retried after Busy, 1850 write stalls (6098 ms); 0.20× the scaling gate's N = 16 (pre-split, balancer off); **under a third of the scaling gate's N = 16: file an issue** | - | `contended-buffered-app-owned.json` |
| Application-owned mode on the defaults, default durability, GroupSync (reported, not judged; D247) | 393K commits/s, p50 330 µs, p99 3981 µs; tablets on 16 of 16 shards after a 39 s warmup; 3957479 two-phase rounds, 48256 refused; 0 commits retried after Busy, 5 write stalls (205 ms) | - | `contended-default-app-owned.json` |
| Contended ycsb-a with riders, D108 parity, buffered (reported, not judged; D247) | snapshot scans p99 403 µs (1056 snapshots), durable-snapshot scans p99 381 µs (1061); durable backup 29642 ms, compaction 2013 ms; close 666 ms; foreground p99 68 µs, 1.35× the judged ycsb-a's | - | `contended-buffered-ycsb-a-riders.json` |
| Contended ycsb-a with riders, default durability, GroupSync (reported, not judged; D247) | snapshot scans p99 256 µs (631 snapshots), durable-snapshot scans p99 258 µs (646); durable backup 36403 ms, compaction 1942 ms; close 433 ms; foreground p99 130 µs, 1.23× the judged ycsb-a's | - | `contended-default-ycsb-a-riders.json` |
| Contended transfer, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 4695 ops/s, p99 44827 µs; 1.169 conflicts per transfer, 0.786 pending-share overlaps retried in the engine (D249); rocksdb 105.71× throughput, p99 135.11×, sqlite 8.85× throughput, p99 133.46×; **> 3× worse than cross-shard-contention against RocksDB (105.71× vs 1.34×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 108K ops/s, p99 1335 µs; rocksdb 0.32× throughput, p99 0.41×, sqlite 0.79× throughput, p99 0.42× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 168K ops/s, p99 1360 µs; rocksdb 0.33× throughput, p99 0.51×, sqlite 0.15× throughput, p99 0.30× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, D108 parity, buffered (reported, not judged; D247) | 23K ops/s, p99 6947 µs; rocksdb 1.07× throughput, p99 0.49×, sqlite 0.49× throughput, p99 0.25×; **> 3× worse than time-series-ttl against RocksDB (1.07× vs 0.31×): file an issue** | - | `contended-*-time-series-range.json` |
| Contended transfer, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 6172 ops/s, p99 35652 µs; 1.840 conflicts per transfer, 1.379 pending-share overlaps retried in the engine (D249); rocksdb 8.84× throughput, p99 11.05×, sqlite 3.15× throughput, p99 349.91×; **> 3× worse than cross-shard-contention against RocksDB (8.84× vs 1.03×): file an issue** | - | `contended-*-transfer.json` |
| Contended sparse-wide-deletes, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 90K ops/s, p99 1696 µs; rocksdb 0.43× throughput, p99 0.48×, sqlite 0.60× throughput, p99 0.53× | - | `contended-*-sparse-wide-deletes.json` |
| Contended adjacency-prefix, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 113K ops/s, p99 1614 µs; rocksdb 0.44× throughput, p99 0.60×, sqlite 0.22× throughput, p99 0.35× | - | `contended-*-adjacency-prefix.json` |
| Contended time-series-range, 16 threads on the default options, default durability, GroupSync (reported, not judged; D247) | 21K ops/s, p99 5276 µs; rocksdb 1.03× throughput, p99 0.39×, sqlite 0.61× throughput, p99 0.29× | - | `contended-*-time-series-range.json` |
| Contended runs stopped at their time bound (reported; D247) | buffered-adjacency-prefix (pigeonhole, 129 s); buffered-adjacency-prefix (rocksdb, 131 s); buffered-sparse-wide (rocksdb, 170 s); buffered-time-series-range (pigeonhole, 133 s); buffered-time-series-range (rocksdb, 132 s); buffered-time-series-range (sqlite-eav, 134 s); buffered-time-series-ttl (rocksdb, 158 s); default-adjacency-prefix (pigeonhole, 131 s); default-adjacency-prefix (rocksdb, 133 s); default-sparse-wide (rocksdb, 175 s); default-time-series-range (rocksdb, 133 s) | - | `contended-*.json` |
| Reproducibility: run 1 and run 2 within tolerance (runs of 10 s or more, D243) | 3 metrics outside tolerance; 1 with different background work (D233) | **FAIL** | `all-uring-compare.txt` |
| Phase 2 floors hold (D193 sparse-wide gate, both runs) |  | **pass** | `phase2-gate.json, phase2-gate-2.json, logs/phase2-gate*.log` |
| #387: row-read and scan p99 within 1.5× of SQLite EAV | run 1: row read 1.28×, scan 1.12×; run 2: row read 1.34×, scan 1.22× | **pass** | `phase2-gate.json, phase2-gate-2.json` |
| D193 instruction ceilings | measured by CI's instructions job, not here | see CI | `-` |

## Pigeonhole against RocksDB (ratio > 1: RocksDB ahead)

| Run | Workload | RocksDB/Pigeonhole throughput | Pigeonhole/RocksDB p99 | Within 1.5× |
|---|---|---:|---:|---|
| all-uring-1 | adjacency | 0.57 | 0.50 | yes |
| all-uring-1 | cross-shard-contention | 0.96 | 1.33 | yes |
| all-uring-1 | group-commit | 0.34 | 0.47 | yes |
| all-uring-1 | skewed-multi-shard | 0.97 | 1.10 | yes |
| all-uring-1 | sparse-wide | 0.69 | 0.46 | yes |
| all-uring-1 | time-series-ttl | 0.94 | 0.71 | yes |
| all-uring-1 | ycsb-a | 0.58 | 0.52 | yes |
| all-uring-1 | ycsb-b | 0.31 | 0.58 | yes |
| all-uring-1 | ycsb-c | 0.41 | 1.07 | yes |
| all-uring-1 | ycsb-d | 0.43 | 1.00 | yes |
| all-uring-1 | ycsb-e | 0.82 | 0.96 | yes |
| all-uring-1 | ycsb-f | 0.84 | 0.71 | yes |
| all-uring-2 | adjacency | 0.46 | 0.39 | yes |
| all-uring-2 | cross-shard-contention | 0.97 | 1.28 | yes |
| all-uring-2 | group-commit | 0.44 | 0.72 | yes |
| all-uring-2 | skewed-multi-shard | 0.88 | 0.91 | yes |
| all-uring-2 | sparse-wide | 0.73 | 0.50 | yes |
| all-uring-2 | time-series-ttl | 0.91 | 0.70 | yes |
| all-uring-2 | ycsb-a | 0.56 | 0.51 | yes |
| all-uring-2 | ycsb-b | 0.30 | 0.59 | yes |
| all-uring-2 | ycsb-c | 0.42 | 1.11 | yes |
| all-uring-2 | ycsb-d | 0.42 | 0.99 | yes |
| all-uring-2 | ycsb-e | 0.76 | 0.81 | yes |
| all-uring-2 | ycsb-f | 0.93 | 0.89 | yes |

## Contended, 16 threads on the default options (D247; ratio > 1: the other store ahead)

| Workload | buffered: Pigeonhole ops/s | buffered: vs RocksDB (tput, p99) | buffered: vs SQLite (tput, p99) | buffered: within 1.5× | default: Pigeonhole ops/s | default: vs RocksDB (tput, p99) | default: vs SQLite (tput, p99) | default: within 1.5× |
|---|--:|---|---|---|--:|---|---|---|
| ycsb-a | 1152296 | 0.62, 0.77 | 0.07, 0.04 | yes | 443859 | 0.43, 0.50 | 0.09, 1.28 | yes |
| ycsb-b | 1634526 | 0.39, 0.74 | 0.14, 0.03 | yes | 1334870 | 0.33, 0.43 | 0.12, 0.07 | yes |
| ycsb-c | 2289286 | 0.70, 1.32 | 0.09, 0.16 | yes | 2304933 | 0.61, 1.19 | 0.09, 0.15 | yes |
| ycsb-e | 33150 | 3.90, 5.76 | 1.34, 0.96 | **no** | 32921 | 3.27, 4.24 | 1.28, 0.46 | **no** |
| ycsb-f | 550726 | 1.17, 1.27 | 0.15, 0.08 | yes | 312014 | 0.55, 0.69 | 0.12, 2.04 | **no** |
| sparse-wide | 72107 | 1.03, 0.84 | 0.57, 0.75 | yes | 74232 | 0.80, 0.70 | 0.46, 0.29 | yes |
| time-series-ttl | 1210441 | 0.31, 0.15 | 0.06, 0.02 | yes | 545223 | 0.45, 0.55 | 0.07, 1.02 | yes |
| cross-shard-contention | 475296 | 1.34, 2.95 | 0.08, 0.10 | **no** | 106878 | 1.03, 1.18 | 0.17, 2.29 | **no** |
| counters | 157584 | 3.42, 7.62 | 0.54, 82.16 | **no** | 81966 | 1.30, 7.12 | 0.33, 51.52 | **no** |

The judged p99 above is over every operation. Each operation type's p99 against the same type in each store (reported, not judged; D261; > 1.5× in bold):

| Workload | Level | Operation | Pigeonhole p99 µs | vs RocksDB p99 | vs SQLite p99 |
|---|---|---|--:|--:|--:|
| ycsb-a | buffered | put | 16.9 | 0.86 | 0.00 |
| ycsb-a | buffered | row read | 57.3 | 0.78 | **1.87** |
| ycsb-a | default | put | 119.3 | 0.54 | 0.35 |
| ycsb-a | default | row read | 38.1 | 0.54 | **2.04** |
| ycsb-b | buffered | put | 15.9 | 1.15 | 0.00 |
| ycsb-b | buffered | row read | 36.9 | 0.74 | 0.39 |
| ycsb-b | default | put | 133.1 | 0.13 | 0.00 |
| ycsb-b | default | row read | 35.6 | 0.51 | 0.86 |
| ycsb-c | buffered | row read | 28.2 | 1.32 | 0.16 |
| ycsb-c | default | row read | 27.5 | 1.19 | 0.15 |
| ycsb-e | buffered | put | 79.4 | **3.21** | 0.00 |
| ycsb-e | buffered | scan | 1581.1 | **5.72** | **2.13** |
| ycsb-e | default | put | 204.8 | 0.07 | 0.00 |
| ycsb-e | default | scan | 1572.9 | **4.44** | **2.56** |
| ycsb-f | buffered | read-modify-write | 94.7 | **2.45** | 0.03 |
| ycsb-f | buffered | row read | 56.3 | 0.75 | **1.82** |
| ycsb-f | default | read-modify-write | 182.3 | 0.65 | 0.98 |
| ycsb-f | default | row read | 41.2 | 0.58 | **2.24** |
| sparse-wide | buffered | get | 34.0 | 1.11 | 0.46 |
| sparse-wide | buffered | put | 497.7 | **30.14** | 0.03 |
| sparse-wide | buffered | row read | 3145.7 | 0.83 | 1.30 |
| sparse-wide | buffered | scan | 3326.0 | 0.86 | 1.25 |
| sparse-wide | default | get | 31.0 | 0.90 | 0.62 |
| sparse-wide | default | put | 667.6 | 0.26 | 0.02 |
| sparse-wide | default | row read | 2867.2 | 0.67 | **2.20** |
| sparse-wide | default | scan | 3014.7 | 0.68 | **2.04** |
| time-series-ttl | buffered | get | 13.6 | 0.62 | 0.50 |
| time-series-ttl | buffered | put at | 27.0 | 1.36 | 0.00 |
| time-series-ttl | buffered | scan | 58.1 | 0.17 | **1.97** |
| time-series-ttl | default | get | 7.6 | 0.72 | 0.46 |
| time-series-ttl | default | put at | 140.3 | 0.63 | 0.04 |
| time-series-ttl | default | scan | 22.4 | 0.36 | 1.22 |
| cross-shard-contention | buffered | cross-shard commit | 112.6 | **2.95** | 0.09 |
| cross-shard-contention | buffered | put | 101.9 | **2.93** | 0.30 |
| cross-shard-contention | default | cross-shard commit | 255.0 | 1.20 | **2.05** |
| cross-shard-contention | default | put | 233.5 | 1.13 | **2.76** |
| counters | buffered | counter read | 10813.4 | **7.46** | **840.66** |
| counters | buffered | incr | 20.2 | 0.64 | 0.43 |
| counters | default | counter read | 17170.4 | **9.70** | **1972.93** |
| counters | default | incr | 712.7 | **3.45** | **9.73** |

## Decisions this window settles

**Row cache (D216):** off (row-cache step) against the default, on (latency step).

| Workload | Throughput off → on | p50 off → on | p99 off → on | Hits (on) |
|---|---|---|---|---|
| ycsb-c | 379991 → 571260 | 2.25 → 0.80 µs | 4.9 → 6.0 µs | 74% |
| ycsb-a | 176527 → 173374 | 4.64 → 4.89 µs | 13.0 → 15.0 µs | 33% |

**ycsb-a, one client (D198 item 2, #478):** Pigeonhole by shard count and shard spin, RocksDB alongside.

| Run | ops/s | p50 µs | p99 µs | parks, wakes per op |
|---|--:|--:|--:|---|
| default | 179673 | 4.9 | 14.4 | shard_parks 0.02, shard_wakes 0.02, commit_parks 0.00 |
| shards1 | 189426 | 4.6 | 13.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| shards4 | 181152 | 4.7 | 14.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin0 | 132944 | 8.4 | 14.8 | shard_parks 0.50, shard_wakes 0.50, commit_parks 0.00 |
| spin100 | 174175 | 4.9 | 14.8 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| spin500 | 182988 | 4.8 | 14.4 | shard_parks 0.00, shard_wakes 0.00, commit_parks 0.00 |
| RocksDB | 100669 | 6.4 | 28.7 | - |

**I/O backend (#402):** io_uring against pread, Pigeonhole throughput and p99.

| Workload | uring/pread throughput | uring/pread p99 |
|---|---:|---:|
| adjacency | 1.00 | 0.99 |
| cross-shard-contention | 0.99 | 1.00 |
| group-commit | 1.39 | 0.63 |
| skewed-multi-shard | 0.88 | 1.19 |
| sparse-wide | 1.03 | 1.00 |
| time-series-ttl | 0.98 | 1.04 |
| ycsb-a | 0.97 | 1.04 |
| ycsb-b | 1.04 | 0.98 |
| ycsb-c | 1.01 | 1.00 |
| ycsb-d | 1.01 | 1.00 |
| ycsb-e | 1.00 | 1.04 |
| ycsb-f | 1.18 | 0.80 |

**Scan readahead (#431):** cold full scan, median MiB/s over runs.

| io | direct | readahead | MiB/s |
|---|---|---|---:|
| pread | 0 | 16 (D222) | 3448 |
| pread | 1 | 16 (D222) | 1029 |
| uring | 0 | 16 (D222) | 3113 |
| uring | 1 | 16 (D222) | 1021 |

**WAL pin bound (#527):** `wal_pin_bytes` at 2× or 4× the memtable budget; mixed-temperature commits (#175's workload), then a reopen after an exit without close. Medians over rounds.

| wal_pin_bytes | Commits/s | Unpin passes | Small flushes | All flushes | WAL files at most | WAL at the crash | Reopen (replay) | First read |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 2× budget | 419861 | 204 | 421 | 228 | 1216 MiB | 1216 MiB | 148 ms | 320 µs |
| 4× budget | 396993 | 107 | 136 | 218 | 1472 MiB | 1472 MiB | 567 ms | 180 µs |

The WAL files' size counts whole preallocated segments (the disk the WAL holds), not live records; the reopen replays what the crash left past each stream's checkpoint.

## Step durations

| Step | Minutes | Commit |
|---|---:|---|
| setup | 0 | 516b59b8f230 |
| build | 2 | 516b59b8f230 |
| memtable-lookup | 1 | 516b59b8f230 |
| phase2-gate | 3 | 516b59b8f230 |
| phase2-gate-2 | 3 | 516b59b8f230 |
| all-uring-1 | 17 | 516b59b8f230 |
| all-uring-2 | 18 | 516b59b8f230 |
| all-pread | 18 | 516b59b8f230 |
| latency | 6 | 516b59b8f230 |
| ycsb-a-shards | 3 | 516b59b8f230 |
| row-cache | 0 | 516b59b8f230 |
| scaling | 8 | 516b59b8f230 |
| placement | 1 | 516b59b8f230 |
| contended | 202 | 516b59b8f230 |
| open-latency | 0 | 516b59b8f230 |
| wal-pin | 0 | 516b59b8f230 |
| cold-get | 36 | 516b59b8f230 |
| cold-scan | 2 | 516b59b8f230 |
| scan-cache | 0 | 516b59b8f230 |
| **total** | **320** | |
