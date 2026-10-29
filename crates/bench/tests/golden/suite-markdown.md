<!-- 2026-10-21/all-uring-1.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.03 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1885336 | 1 | 176.4K | 4.86 | 14.9 | 21.4 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1001000 | 1 | 100.0K | 6.37 | 29.3 | 36.6 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3060164 | 1 | 282.0K | 1.05 | 13.3 | 16.2 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 89.4K | 9.21 | 22.8 | 26.2 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4055815 | 1 | 388.6K | 0.90 | 12.2 | 14.8 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1688502 | 1 | 162.6K | 5.41 | 10.9 | 13.6 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3400014 | 1 | 302.0K | 1.09 | 14.1 | 18.8 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1283884 | 1 | 122.4K | 7.87 | 15.1 | 17.5 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 14.5K | 66.6 | 181 | 238 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.8K | 81.9 | 185 | 194 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 88.7K | 10.8 | 27.9 | 34.0 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 86.2K | 10.2 | 29.8 | 37.9 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 27.1K | 11.1 | 408 | 502 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 18.5K | 6.72 | 930 | 1245 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2648837 | 1 | 242.4K | 4.19 | 7.58 | 10.7 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2405697 | 1 | 214.7K | 3.25 | 12.2 | 14.7 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 26.3K | 11.8 | 293 | 336 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 15.1K | 16.9 | 586 | 647 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 8098696 | 4 | 748.9K | 4.93 | 9.21 | 13.8 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6932550 | 4 | 641.7K | 6.33 | 10.4 | 13.6 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 26000000 | 16 | 2.56M | 73.7 | 137 | 918 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 26000000 | 16 | 2.48M | 76.8 | 167 | 315 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.4K | 49.4 | 118 | 157 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 751022 | 4 | 68.8K | 45.8 | 140 | 166 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 2319185 | 16 | 231.9K | 56.3 | 162 | 418 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.4K | 63.2 | 78.3 | 101 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 291119 | 4 | 28.3K | 143 | 169 | 200 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 1054191 | 16 | 96.9K | 175 | 230 | 293 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 7728386 | 32 | 645.7K | 36.4 | 147 | 457 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7937561 | 32 | 640.3K | 36.4 | 145 | 520 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1000000 | 6637306 | 32 | 645.1K | 36.4 | 146 | 594 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6360335 | 32 | 570.8K | 50.7 | 103 | 326 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2304 MiB | 0 | 163531 | 9.28 | 17.9 | 49.4 | 778741 | 6.11 | 15.7 | 23.6 |
| ycsb-a | rocksdb | 1256 MiB | 0 | 109818 | 10.9 | 16.3 | 18.9 | 390572 | 18.8 | 32.1 | 40.2 |
| ycsb-b | pigeonhole | 2048 MiB | 0 | 275764 | 8.38 | 14.2 | 17.5 | 2631386 | 0.94 | 12.7 | 15.7 |
| ycsb-b | rocksdb | 1211 MiB | 0 | 158422 | 8.89 | 12.8 | 15.4 | 791650 | 9.73 | 23.6 | 26.4 |
| ycsb-c | pigeonhole | 1728 MiB | 0 | 310334 | 7.55 | 13.4 | 17.3 | 3745481 | 0.81 | 11.6 | 14.6 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 215787 | 8.06 | 11.7 | 14.1 | 1472715 | 4.35 | 10.8 | 13.4 |
| ycsb-d | pigeonhole | 2176 MiB | 0 | 545333 | 7.74 | 14.5 | 26.5 | 2684923 | 0.88 | 11.8 | 16.8 |
| ycsb-d | rocksdb | 1289 MiB | 0 | 278149 | 10.2 | 15.6 | 18.3 | 941490 | 5.79 | 15.0 | 17.3 |
| ycsb-f | pigeonhole | 2240 MiB | 0 | 110217 | 9.28 | 16.3 | 23.3 | 389488 | 5.31 | 14.3 | 19.5 |
| ycsb-f | rocksdb | 1254 MiB | 0 | 110217 | 10.6 | 16.1 | 18.8 | 389488 | 19.1 | 32.6 | 41.0 |
| sparse-wide | pigeonhole | 2816 MiB | 0 | 122676 | 3.97 | 16.8 | 20.9 | 477384 | 3.89 | 389 | 461 |
| sparse-wide | rocksdb | 2123 MiB | 0 | 122676 | 6.62 | 15.4 | 18.2 | 477384 | 5.73 | 918 | 1229 |
| time-series-ttl | pigeonhole | 576 MiB | 0 | 432402 | 1.42 | 4.19 | 6.05 | 96565 | 1.24 | 4.03 | 5.50 |
| time-series-ttl | rocksdb | 255 MiB | 0 | 393501 | 1.16 | 5.05 | 7.10 | 86738 | 1.05 | 4.35 | 6.30 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 943064 | 4.90 | 4.64 | 7.62 | 11.3 |
| ycsb-a | pigeonhole | row read | 942272 | 6.35 | 7.04 | 16.2 | 26.1 |
| ycsb-a | rocksdb | put | 500610 | 3.35 | 3.15 | 5.89 | 8.32 |
| ycsb-a | rocksdb | row read | 500390 | 16.6 | 14.2 | 31.4 | 39.2 |
| ycsb-b | pigeonhole | put | 153014 | 9.44 | 10.0 | 14.6 | 32.0 |
| ycsb-b | pigeonhole | row read | 2907150 | 3.18 | 1.01 | 13.2 | 15.9 |
| ycsb-b | rocksdb | put | 49928 | 3.89 | 3.62 | 7.04 | 9.28 |
| ycsb-b | rocksdb | row read | 950072 | 11.5 | 9.41 | 22.9 | 26.2 |
| ycsb-d | pigeonhole | put | 169758 | 9.44 | 8.26 | 17.9 | 22.1 |
| ycsb-d | pigeonhole | row read | 3230256 | 2.93 | 1.04 | 13.1 | 17.4 |
| ycsb-d | rocksdb | put | 64245 | 7.71 | 7.49 | 11.6 | 14.2 |
| ycsb-d | rocksdb | row read | 1219639 | 8.13 | 7.97 | 15.2 | 17.7 |
| ycsb-e | pigeonhole | put | 49830 | 35.4 | 36.1 | 42.5 | 48.9 |
| ycsb-e | pigeonhole | scan | 950170 | 70.5 | 69.1 | 182 | 239 |
| ycsb-e | rocksdb | put | 49830 | 9.48 | 9.34 | 13.9 | 17.0 |
| ycsb-e | rocksdb | scan | 950170 | 88.6 | 86.0 | 185 | 194 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 16.5 | 15.9 | 29.2 | 36.4 |
| ycsb-f | pigeonhole | row read | 499705 | 5.88 | 6.11 | 15.1 | 20.2 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.52 | 4.80 | 14.5 | 17.7 |
| ycsb-f | rocksdb | row read | 499705 | 16.6 | 14.1 | 31.9 | 40.2 |
| sparse-wide | pigeonhole | get | 399998 | 3.73 | 3.09 | 11.6 | 15.0 |
| sparse-wide | pigeonhole | put | 200020 | 13.4 | 12.2 | 37.4 | 62.2 |
| sparse-wide | pigeonhole | row read | 200062 | 62.4 | 15.1 | 436 | 471 |
| sparse-wide | pigeonhole | scan | 199920 | 101 | 69.6 | 483 | 520 |
| sparse-wide | rocksdb | get | 399998 | 4.18 | 4.48 | 9.60 | 11.9 |
| sparse-wide | rocksdb | put | 200020 | 5.12 | 4.83 | 9.86 | 12.1 |
| sparse-wide | rocksdb | row read | 200062 | 112 | 15.0 | 1065 | 1270 |
| sparse-wide | rocksdb | scan | 199920 | 144 | 47.4 | 1130 | 1294 |
| time-series-ttl | pigeonhole | get | 528967 | 1.71 | 1.40 | 4.16 | 5.95 |
| time-series-ttl | pigeonhole | put at | 1060050 | 4.84 | 4.45 | 7.58 | 10.7 |
| time-series-ttl | pigeonhole | scan | 1059820 | 4.51 | 4.29 | 7.93 | 11.8 |
| time-series-ttl | rocksdb | get | 480239 | 1.67 | 1.14 | 4.96 | 7.04 |
| time-series-ttl | rocksdb | put at | 963099 | 3.06 | 2.93 | 5.12 | 7.97 |
| time-series-ttl | rocksdb | scan | 962359 | 7.63 | 7.10 | 13.0 | 16.3 |
| adjacency | pigeonhole | put | 99878 | 9.39 | 9.92 | 14.3 | 17.4 |
| adjacency | pigeonhole | scan | 900122 | 41.1 | 13.8 | 297 | 338 |
| adjacency | rocksdb | put | 99878 | 4.51 | 4.35 | 7.78 | 10.2 |
| adjacency | rocksdb | scan | 900122 | 73.2 | 21.0 | 590 | 647 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3864701 | 61.2 | 45.1 | 162 | 700 |
| cross-shard-contention | pigeonhole | put | 3863685 | 36.7 | 26.6 | 119 | 293 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3969365 | 61.3 | 45.1 | 160 | 885 |
| cross-shard-contention | pigeonhole | put | 3968196 | 36.1 | 26.6 | 118 | 297 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3319772 | 60.3 | 45.3 | 162 | 840 |
| cross-shard-contention | pigeonhole | put | 3317534 | 36.1 | 26.6 | 118 | 340 |
| cross-shard-contention | rocksdb | cross-shard commit | 3181480 | 57.5 | 52.2 | 106 | 326 |
| cross-shard-contention | rocksdb | put | 3178855 | 53.0 | 49.4 | 99.8 | 324 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: moved, room, pending overlap | Seqnos used | Inferred wasted rounds | Share waits, ms held |
|---|---|---|--:|--:|--:|--:|---|--:|--:|---|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7728386 | 3864701 | 262 | 0.000 | 0, 0, 262 | 11593349 | 3864963 | 130, 1.4 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7937561 | 3969365 | 267 | 0.000 | 0, 0, 267 | 11907193 | 3969632 | 157, 1.6 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6637306 | 3319772 | 242 | 0.000 | 0, 0, 242 | 9957320 | 3320014 | 149, 1.6 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 503358 | 53.4% | 5 → 5 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 439706 | 46.6% | 4 → 4 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 24091 | 15.7% | 2 → 2 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 27450 | 17.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 65454 | 42.8% | 4 → 4 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 12277 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 23742 | 15.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 3 → 3 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 117683 | 69.3% | 6 → 6 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 52075 | 30.7% | 3 → 3 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 6513 | 13.1% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 4973 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 17373 | 34.9% | 2 → 3 | 1 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 13687 | 27.5% | 3 → 3 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 7284 | 14.6% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 406546 | 81.3% | 1 → 7 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 5862 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 32723 | 6.5% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 14619 | 2.9% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 25646 | 5.1% | 2 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 14899 | 3.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 48163 | 24.1% | 9 → 9 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 51079 | 25.5% | 13 → 14 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 53759 | 26.9% | 10 → 10 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 44148 | 22.1% | 9 → 12 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2166 | 1.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 150 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 50 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 72 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 160 | 0.1% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 107 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 137 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 29 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 319379 | 30.1% | 6 → 9 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 740671 | 69.9% | 14 → 11 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 99878 | 100.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 574850 | 7.1% | 2 → 9 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 605979 | 7.5% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 456970 | 5.6% | 1 → 7 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 388329 | 4.8% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 453880 | 5.6% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 496647 | 6.1% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 555072 | 6.9% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 450493 | 5.6% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 482371 | 6.0% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 539736 | 6.7% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 590096 | 7.3% | 4 → 3 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 496203 | 6.1% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 447440 | 5.5% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 400881 | 4.9% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 592681 | 7.3% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 567068 | 7.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 0 | 1608542 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1 | 1633411 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 2 | 1611727 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 3 | 1633073 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 4 | 1614340 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 5 | 1633762 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 6 | 1616173 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 7 | 1636024 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 8 | 1607775 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 9 | 1641185 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 10 | 1618032 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 11 | 1637779 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 12 | 1613807 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 13 | 1644513 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 14 | 1609803 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 15 | 1640054 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 0 | 1608542 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1 | 1633411 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 2 | 1611727 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 3 | 1633073 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 4 | 1614340 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 5 | 1633762 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 6 | 1616173 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 7 | 1636024 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 8 | 1607775 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 9 | 1641185 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 10 | 1618032 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 11 | 1637779 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 12 | 1613807 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 13 | 1644513 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 14 | 1609803 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 15 | 1640054 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 8927 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 13941 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 20881 | 10.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 8861 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 11533 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 16412 | 8.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 2988 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 14903 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 4632 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 5633 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 12465 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 6866 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 12522 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 30596 | 15.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 10556 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 18284 | 9.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 26144 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 29153 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 52061 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 40155 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 24784 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 28592 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 36719 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 100141 | 13.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 52457 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 65369 | 8.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 15093 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 38995 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 23536 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 65236 | 8.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 35076 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 117511 | 15.6% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 141400 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 143384 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 138829 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 138981 | 6.0% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 187541 | 8.1% | 2 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 144163 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 165205 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 126785 | 5.5% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 123122 | 5.3% | 1 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 148583 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 122148 | 5.3% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 167771 | 7.2% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 118215 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 183804 | 7.9% | 2 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 127645 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 141609 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 483844 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 484483 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 481495 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 484167 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 482422 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 482976 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 483293 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 482068 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 482634 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 482775 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 482134 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 483142 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 483962 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 482638 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 482493 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 483860 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 496836 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 497609 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 494464 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 497193 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 495533 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 496171 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 496532 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 495045 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 495634 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 495927 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 495272 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 496169 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 496949 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 495732 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 495611 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 496884 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 0 | 415538 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1 | 416062 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 2 | 413666 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 3 | 415714 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 4 | 414327 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 5 | 415471 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6 | 415050 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7 | 413801 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 8 | 414338 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 9 | 414694 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 10 | 413907 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 11 | 414795 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 12 | 415802 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 13 | 413950 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 14 | 414458 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 15 | 415733 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 11 | 4 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.04 | 0.04 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 18 | 6 | 1 (2) | 0 | 0 | 0 | 0.0 | 0.02 | 0.02 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 2 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 8 | 2 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.50 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 5 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.19 | 0.19 | 0.00 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 2 (0 / 0) | 79.8 (0.0 / 0.0) | 10 | 1 | 1 (3) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.08 | 0.08 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 99 | 27 | 1 (2) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.00 | 105 (0) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 1052 | 258 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.08 | 0.00 | 307509 (56958) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 1062 | 264 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.10 | 0.00 | 2936080 (720278) | 0 |
| group-commit | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 2 | 2 | 0 (0) | 0 | 0 | 1 | 0.1 | 1.91 | 1.96 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 28 | 2 | 0 (0) | 0 | 0 | 5 | 0.4 | 0.96 | 1.42 | 1.00 | 2199 (18) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 95 | 26 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.82 | 1.00 | 49021 (4911) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 160 | 48 | 0 (0) | 0 | 0 | 8 | 8.8 | 0.00 | 0.15 | 0.14 | 805201 (476695) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 176 | 48 | 0 (0) | 0 | 0 | 17 | 9.9 | 0.00 | 0.15 | 0.14 | 801317 (486709) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 144 | 32 | 0 (0) | 0 | 0 | 4 | 5.1 | 0.00 | 0.15 | 0.15 | 704920 (421091) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 10.24M cells/s (639.7K per core), commit p50 73.7 µs, p99 137 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 65.5 µs, p99 98.3 µs, p99.9 786 µs; 0 WAL rollover blocks, 0 inline WAL syncs — pass.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 9.91M cells/s, commit p50 76.8 µs, p99 167 µs.

<!-- 2026-10-21/contended-buffered-cross-shard-contention.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.92 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 33404828 | 16 | 536.5K | 23.0 | 108 | 210 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 34881868 | 16 | 552.3K | 22.5 | 108 | 205 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1000000 | 32993368 | 16 | 539.0K | 23.2 | 108 | 198 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 42260712 | 16 | 636.1K | 24.7 | 35.6 | 54.3 |
| cross-shard-contention | sqlite-eav | page_cache=320MiB buffered | 1000000 | 2297708 | 16 | 37.4K | 17.5 | 1114 | 78643 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | cross-shard commit | 16697500 | 39.2 | 30.1 | 120 | 354 |
| cross-shard-contention | pigeonhole | put | 16707328 | 19.9 | 15.4 | 84.0 | 138 |
| cross-shard-contention | pigeonhole | cross-shard commit | 17436791 | 38.2 | 29.3 | 121 | 356 |
| cross-shard-contention | pigeonhole | put | 17445077 | 19.3 | 14.9 | 84.5 | 139 |
| cross-shard-contention | pigeonhole | cross-shard commit | 16491849 | 39.2 | 30.2 | 121 | 313 |
| cross-shard-contention | pigeonhole | put | 16501519 | 19.8 | 15.4 | 84.5 | 137 |
| cross-shard-contention | rocksdb | cross-shard commit | 21125758 | 25.4 | 25.0 | 36.9 | 56.6 |
| cross-shard-contention | rocksdb | put | 21134954 | 24.7 | 24.4 | 34.0 | 51.5 |
| cross-shard-contention | sqlite-eav | cross-shard commit | 1148605 | 408 | 25.3 | 1237 | 78643 |
| cross-shard-contention | sqlite-eav | put | 1149103 | 382 | 11.7 | 340 | 78643 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: moved, room, pending overlap | Seqnos used | Inferred wasted rounds | Share waits, ms held |
|---|---|---|--:|--:|--:|--:|---|--:|--:|---|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 33404828 | 16697500 | 672 | 0.000 | 0, 0, 672 | 50103000 | 16698172 | 403, 3.2 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 34881868 | 17436791 | 649 | 0.000 | 0, 0, 649 | 52319308 | 17437440 | 380, 11.0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 32993368 | 16491849 | 657 | 0.000 | 0, 0, 657 | 49485874 | 16492506 | 414, 16.6 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2089861 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 2088989 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2084095 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2088727 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2085225 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 2088223 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2090525 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 2086399 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 2085113 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2088256 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 2086768 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 2089683 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 2088058 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 2086287 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 2089114 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2089505 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 2182199 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 2181017 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 2176405 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 2180762 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 2177689 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 2180848 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 2182893 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 2178911 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 2177521 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 2180521 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 2179437 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 2181707 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 2180356 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 2178810 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 2181256 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 2181536 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 0 | 2064011 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1 | 2063304 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 2 | 2058202 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 3 | 2063004 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 4 | 2059538 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 5 | 2062602 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6 | 2064916 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7 | 2060758 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 8 | 2059673 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 9 | 2062523 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 10 | 2060942 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 11 | 2064108 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 12 | 2062263 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 13 | 2060631 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 14 | 2062998 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 15 | 2063895 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 704 | 176 | 0 (0) | 0 | 0 | 12 | 280.1 | 0.00 | 0.18 | 0.05 | 1078764 (341654) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 736 | 192 | 0 (0) | 0 | 0 | 12 | 95.7 | 0.00 | 0.19 | 0.05 | 1124456 (343653) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 688 | 176 | 0 (0) | 0 | 0 | 11 | 147.8 | 0.00 | 0.18 | 0.05 | 1085919 (347697) | 0 |

<!-- 2026-10-13/scaling-4.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.83 [non-reference (D5)]
Build: release, rev 8654ad6924a9. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 1000000 | 4000000 | 1 | 418.7K | 15.9 | 24.8 | 32.8 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 1000000 | 16000000 | 4 | 1.84M | 18.0 | 30.6 | 88.1 |
| skewed-multi-shard | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 1000000 | 4000000 | 1 | 444.6K | 15.8 | 25.5 | 31.4 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 1000000 | 16000000 | 4 | 1.83M | 18.8 | 33.0 | 99.3 |
| skewed-multi-shard | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 8 | 337.9K | 9.98 | 518 | 557 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 8 | 546.4K | 8.57 | 25.3 | 537 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 0 | 4066712 | 25.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 1 | 4058328 | 25.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 2 | 4051056 | 25.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 pinned | 3 | 3823904 | 23.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 0 | 4066712 | 25.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 1 | 4058328 | 25.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 2 | 4051056 | 25.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on presplit=writes inline in-flight=16 unpinned | 3 | 3823904 | 23.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 791592 | 39.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1208408 | 60.4% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 41 | 10 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.05 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 163 | 42 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.06 | 0.00 | 607089 (6442) | 0 |
| skewed-multi-shard | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 41 | 10 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.05 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 163 | 43 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.06 | 0.00 | 1252634 (12961) | 0 |
| skewed-multi-shard | pigeonhole | 8 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 21 | 6 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.05 | 0.07 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 8 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 20 | 5 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.05 | 0.03 | 203 (1) | 0 |

**Scaling gate:** 4 shards reach 1.84M ops/s against 418.7K ops/s on one shard: efficiency 1.10 (gate ≥ 0.8); the table pre-split at the writes' quantiles, the busiest shard took 25% of the commits; 0.038 visibility waits per operation — pass.

**Scaling with the shard threads unpinned (reported, not judged; D235):** 4 shards reach 1.83M ops/s against 444.6K ops/s on one shard: efficiency 1.03.

**Engine-owned, synchronous clients (reported, not gating; D204):** 2 shards with 4 clients each reach 546.4K ops/s against 337.9K ops/s on one shard: efficiency 0.81.

<!-- 2026-10-11/scaling-4.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.06 [non-reference (D5)]
Build: release, rev 8f675ed0c433. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 1000000 | 2000000 | 1 | 380.1K | 15.6 | 270 | 328 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 1000000 | 2000000 | 4 | 1.56M | 19.3 | 118 | 291 |
| skewed-multi-shard | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 8 | 356.0K | 9.86 | 514 | 549 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 8 | 525.7K | 8.19 | 27.0 | 532 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 0 | 554117 | 27.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 1 | 634906 | 31.7% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 2 | 365059 | 18.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 | 3 | 445918 | 22.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 924773 | 46.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=2 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1075227 | 53.8% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 1 | 9700 (0 / 0) | 1482.8 (0.0 / 0.0) | 21 | 6 | 0 (0) | 0 | 0 | 1 | 46.4 | 0.00 | 0.06 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 1369 (0 / 0) | 200.9 (0.0 / 0.0) | 19 | 3 | 0 (0) | 0 | 0 | 2 | 1.1 | 0.00 | 0.07 | 0.00 | 112048 (2081) | 0 |
| skewed-multi-shard | pigeonhole | 8 | 1245 (0 / 0) | 291.8 (0.0 / 0.0) | 21 | 5 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.04 | 0.05 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 8 | 356 (0 / 0) | 81.8 (0.0 / 0.0) | 20 | 4 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.03 | 0.04 | 689 (28) | 0 |

**Scaling gate:** 4 shards reach 1.56M ops/s against 380.1K ops/s on one shard: efficiency 1.03 (gate ≥ 0.8); the table pre-split at the writes' quantiles, the busiest shard took 32% of the commits; 0.056 visibility waits per operation — pass.

**Engine-owned, synchronous clients (reported, not gating; D204):** 2 shards with 4 clients each reach 525.7K ops/s against 356.0K ops/s on one shard: efficiency 0.74.

<!-- 2026-10-13/placement-8.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.43 [non-reference (D5)]
Build: release, rev 8654ad6924a9. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|

**Balancer placement (D231):** 8 shards, open-loop at 240.0K writes/s offered (240.0K completed); settled after 16 s (bound 60 s); the busiest shard took 14.3% of 10 s of writes (at most 15.6%); 0 tablet changes while measuring — pass.

<!-- 2026-10-13/all-uring-1-run1.json -->
**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 0.97 [non-reference (D5)]
Build: release, rev 8654ad6924a9. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 185.5K | 4.74 | 13.8 | 19.1 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 107.6K | 5.89 | 27.4 | 33.0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 293.0K | 1.01 | 12.7 | 15.9 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 93.6K | 9.02 | 22.1 | 25.3 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 378.6K | 0.88 | 11.8 | 14.3 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 157.4K | 5.57 | 11.2 | 14.1 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 319.3K | 1.00 | 13.1 | 16.0 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 131.4K | 7.13 | 14.5 | 16.8 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 13.5K | 71.2 | 183 | 231 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.8K | 81.9 | 185 | 194 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 120.6K | 7.71 | 19.3 | 420 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 91.1K | 9.79 | 28.3 | 36.1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 27.8K | 11.1 | 412 | 561 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 19.7K | 6.46 | 864 | 1245 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 202.1K | 4.29 | 11.8 | 16.9 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 242.8K | 3.13 | 9.73 | 12.6 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 26.6K | 11.8 | 305 | 340 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 16.4K | 16.1 | 549 | 610 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 4 | 362.1K | 7.68 | 16.2 | 532 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2000000 | 4 | 517.6K | 6.43 | 11.7 | 14.6 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 200000 | 16 | 1.24M | 161 | 483 | 5865 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 200000 | 16 | 1.16M | 175 | 385 | 3195 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 18.3K | 48.4 | 116 | 139 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 4 | 59.4K | 51.5 | 137 | 169 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 16 | 120.3K | 135 | 175 | 1081 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.5K | 63.0 | 77.8 | 100 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 4 | 28.1K | 142 | 166 | 199 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 16 | 95.4K | 178 | 234 | 309 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1536 MiB | 0 | 109752 | 9.02 | 17.0 | 25.0 | 390163 | 4.77 | 13.4 | 18.7 |
| ycsb-a | rocksdb | 1243 MiB | 0 | 109752 | 10.2 | 15.7 | 18.0 | 390163 | 16.2 | 29.2 | 36.4 |
| ycsb-b | pigeonhole | 1536 MiB | 0 | 158422 | 7.97 | 14.0 | 18.4 | 791650 | 0.81 | 10.9 | 14.9 |
| ycsb-b | rocksdb | 1209 MiB | 0 | 158422 | 8.96 | 12.9 | 15.5 | 791650 | 9.28 | 22.3 | 25.5 |
| ycsb-c | pigeonhole | 1472 MiB | 0 | 162648 | 7.42 | 13.1 | 16.6 | 837352 | 0.73 | 9.60 | 13.6 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 162648 | 8.38 | 12.1 | 14.7 | 837352 | 4.48 | 11.1 | 13.8 |
| ycsb-d | pigeonhole | 1536 MiB | 0 | 230659 | 7.49 | 13.6 | 17.0 | 719316 | 0.76 | 9.02 | 13.0 |
| ycsb-d | rocksdb | 1267 MiB | 0 | 230659 | 9.60 | 14.9 | 17.3 | 719316 | 5.41 | 14.3 | 16.5 |
| ycsb-f | pigeonhole | 1600 MiB | 0 | 110217 | 8.83 | 15.6 | 20.2 | 389488 | 4.58 | 14.0 | 19.3 |
| ycsb-f | rocksdb | 1243 MiB | 0 | 110217 | 10.2 | 15.6 | 18.3 | 389488 | 16.6 | 31.1 | 39.2 |
| sparse-wide | pigeonhole | 2496 MiB | 0 | 122676 | 3.49 | 16.1 | 19.7 | 477384 | 3.41 | 397 | 514 |
| sparse-wide | rocksdb | 2120 MiB | 0 | 122676 | 6.40 | 15.2 | 17.7 | 477384 | 5.54 | 852 | 1221 |
| time-series-ttl | pigeonhole | 320 MiB | 0 | 166812 | 1.30 | 3.49 | 6.14 | 32311 | 1.10 | 3.12 | 5.28 |
| time-series-ttl | rocksdb | 176 MiB | 0 | 166812 | 1.06 | 4.96 | 6.78 | 32311 | 0.90 | 3.73 | 5.21 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 5.19 | 4.58 | 9.79 | 12.2 |
| ycsb-a | pigeonhole | row read | 499915 | 5.51 | 5.50 | 15.0 | 21.2 |
| ycsb-a | rocksdb | put | 500085 | 3.23 | 3.04 | 5.66 | 8.13 |
| ycsb-a | rocksdb | row read | 499915 | 15.3 | 12.9 | 28.7 | 35.6 |
| ycsb-b | pigeonhole | put | 49928 | 9.37 | 9.54 | 14.1 | 17.5 |
| ycsb-b | pigeonhole | row read | 950072 | 3.06 | 0.96 | 12.7 | 15.7 |
| ycsb-b | rocksdb | put | 49928 | 3.86 | 3.58 | 7.04 | 9.21 |
| ycsb-b | rocksdb | row read | 950072 | 11.0 | 9.15 | 22.1 | 25.3 |
| ycsb-d | pigeonhole | put | 50025 | 11.2 | 11.3 | 16.1 | 22.3 |
| ycsb-d | pigeonhole | row read | 949975 | 2.66 | 0.94 | 12.0 | 14.9 |
| ycsb-d | rocksdb | put | 50025 | 7.42 | 7.17 | 11.2 | 13.8 |
| ycsb-d | rocksdb | row read | 949975 | 7.58 | 7.10 | 14.5 | 16.8 |
| ycsb-e | pigeonhole | put | 49830 | 33.6 | 36.9 | 46.3 | 51.2 |
| ycsb-e | pigeonhole | scan | 950170 | 76.0 | 73.7 | 184 | 232 |
| ycsb-e | rocksdb | put | 49830 | 9.54 | 9.41 | 14.0 | 16.6 |
| ycsb-e | rocksdb | scan | 950170 | 88.6 | 86.0 | 185 | 194 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 11.0 | 9.15 | 21.0 | 510 |
| ycsb-f | pigeonhole | row read | 499705 | 5.45 | 5.41 | 14.7 | 19.7 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.25 | 4.58 | 13.6 | 16.9 |
| ycsb-f | rocksdb | row read | 499705 | 15.6 | 13.1 | 30.2 | 38.4 |
| sparse-wide | pigeonhole | get | 399998 | 3.31 | 2.65 | 11.1 | 14.5 |
| sparse-wide | pigeonhole | put | 200020 | 18.4 | 13.6 | 41.7 | 62.2 |
| sparse-wide | pigeonhole | row read | 200062 | 57.5 | 14.1 | 471 | 524 |
| sparse-wide | pigeonhole | scan | 199920 | 97.2 | 68.1 | 528 | 586 |
| sparse-wide | rocksdb | get | 399998 | 4.03 | 4.35 | 9.21 | 11.4 |
| sparse-wide | rocksdb | put | 200020 | 5.01 | 4.67 | 9.66 | 12.0 |
| sparse-wide | rocksdb | row read | 200062 | 105 | 14.4 | 1065 | 1245 |
| sparse-wide | rocksdb | scan | 199920 | 135 | 46.6 | 1106 | 1303 |
| time-series-ttl | pigeonhole | get | 199123 | 1.55 | 1.28 | 3.46 | 6.08 |
| time-series-ttl | pigeonhole | put at | 400947 | 7.10 | 6.85 | 12.9 | 29.3 |
| time-series-ttl | pigeonhole | scan | 399930 | 4.37 | 4.32 | 7.26 | 9.86 |
| time-series-ttl | rocksdb | get | 199123 | 1.54 | 1.02 | 4.86 | 6.62 |
| time-series-ttl | rocksdb | put at | 400947 | 3.00 | 2.88 | 4.96 | 7.90 |
| time-series-ttl | rocksdb | scan | 399930 | 6.43 | 6.01 | 11.0 | 13.6 |
| adjacency | pigeonhole | put | 99878 | 10.5 | 10.2 | 14.7 | 45.3 |
| adjacency | pigeonhole | scan | 900122 | 40.6 | 13.7 | 307 | 340 |
| adjacency | rocksdb | put | 99878 | 4.34 | 4.06 | 7.58 | 9.86 |
| adjacency | rocksdb | scan | 900122 | 67.3 | 20.1 | 557 | 610 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 219759 | 43.9% | 5 → 6 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 280326 | 56.1% | 13 → 12 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 22536 | 45.1% | 5 → 5 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 27392 | 54.9% | 11 → 11 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 6 → 6 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 10 → 10 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 28631 | 57.2% | 12 → 12 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 21394 | 42.8% | 4 → 4 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 26627 | 53.4% | 11 → 11 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 23203 | 46.6% | 5 → 5 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 291761 | 58.3% | 11 → 15 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 208534 | 41.7% | 5 → 1 | 0 | 0 | 4 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 46857 | 23.4% | 13 → 14 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 26860 | 13.4% | 8 → 10 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 23811 | 11.9% | 7 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 73874 | 36.9% | 10 → 10 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 28042 | 14.0% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 46 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 44 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 64 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 111 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 19 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 14 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 131 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 121 | 0.1% | 3 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 26 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 44580 | 11.1% | 1 → 10 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 163114 | 40.7% | 0 → 10 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 3014 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2993 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 38241 | 9.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 4897 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 48994 | 12.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 34574 | 8.6% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 3492 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 3436 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 6247 | 1.6% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 27917 | 7.0% | 2 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1799 | 0.4% | 1 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 5435 | 1.4% | 2 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 7550 | 1.9% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 4664 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 99760 | 99.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 6 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 6 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 10 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 36 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 4 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 6 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 2 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 6 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 10 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 8 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1678115 | 83.9% | 16 → 13 | 0 | 0 | 3 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 218882 | 10.9% | 0 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 22316 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 80687 | 4.0% | 0 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 0 | 12279 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1 | 12677 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 2 | 12317 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 3 | 12664 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 4 | 12386 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 5 | 12745 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 6 | 12509 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 7 | 12528 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 8 | 12497 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 9 | 12619 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 10 | 12178 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 11 | 12567 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 12 | 12555 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 13 | 12337 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 14 | 12419 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 15 | 12723 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 0 | 12279 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1 | 12677 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 2 | 12317 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 3 | 12664 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 4 | 12386 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 5 | 12745 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 6 | 12509 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 7 | 12528 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 8 | 12497 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 9 | 12619 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 10 | 12178 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 11 | 12567 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 12 | 12555 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 13 | 12337 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 14 | 12419 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 15 | 12723 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 6831 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 41895 | 20.9% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 18427 | 9.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 5241 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 9926 | 5.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 18488 | 9.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 6754 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 22301 | 11.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 9240 | 4.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 3151 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 6753 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 15955 | 8.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 9825 | 4.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 4046 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 7865 | 3.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 13302 | 6.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 6953 | 3.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 57467 | 28.7% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 15348 | 7.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 6388 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 11396 | 5.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 7931 | 4.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 9138 | 4.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 12129 | 6.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 14087 | 7.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 10732 | 5.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 5616 | 2.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 7275 | 3.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 4061 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 11658 | 5.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 8694 | 4.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 11127 | 5.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 5179 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 101931 | 51.0% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 9721 | 4.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 6171 | 3.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 2006 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 4966 | 2.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 4480 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 9143 | 4.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 1841 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 11248 | 5.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 9878 | 4.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 4871 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 8219 | 4.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 7428 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 6602 | 3.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 6316 | 3.2% | 0 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls (room / pacing) | Stalled ms (room / pacing) | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.07 | 0.07 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.05 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 4 | 2 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.12 | 0.12 | 0.00 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 7 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.20 | 0.20 | 0.08 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 19 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.24 | 0.24 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 43 (0 / 0) | 580.4 (0.0 / 0.0) | 82 | 38 | 1 (9) | 0 | 0 | 0 | 0.0 | 0.01 | 0.05 | 0.01 | 79 (0) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 14 | 0 | 0 (0) | 0 | 0 | 10 | 8.2 | 0.00 | 0.12 | 0.00 | 7734 (868) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 15 | 0 | 0 (0) | 0 | 0 | 7 | 0.9 | 0.00 | 0.12 | 0.00 | 5681 (720) | 0 |
| group-commit | pigeonhole | 1 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 2 | 0 | 0 (0) | 0 | 0 | 2 | 0.1 | 1.65 | 1.82 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 3 | 3 | 0 (0) | 0 | 0 | 3 | 0.2 | 1.20 | 1.37 | 1.00 | 399 (1) | 0 |
| group-commit | pigeonhole | 16 | 0 (0 / 0) | 0.0 (0.0 / 0.0) | 2 | 0 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.36 | 0.60 | 1.00 | 4051 (413) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 4.95M cells/s (309.2K per core), commit p50 161 µs, p99 483 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 131 µs, p99 393 µs, p99.9 4194 µs; 0 WAL rollover blocks, 0 inline WAL syncs — fail.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 4.63M cells/s, commit p50 175 µs, p99 385 µs.

