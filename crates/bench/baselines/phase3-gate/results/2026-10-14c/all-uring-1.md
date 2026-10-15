**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 0.99 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1941641 | 1 | 180.1K | 4.77 | 14.7 | 22.3 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1007149 | 1 | 100.5K | 6.30 | 29.6 | 36.6 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3121691 | 1 | 284.7K | 1.05 | 13.4 | 16.0 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 89.9K | 9.21 | 22.4 | 25.6 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4085028 | 1 | 386.2K | 0.92 | 12.1 | 14.6 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1641760 | 1 | 158.5K | 5.54 | 11.1 | 14.0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2940422 | 1 | 278.5K | 1.14 | 14.7 | 20.0 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1249489 | 1 | 119.5K | 7.97 | 15.6 | 18.2 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 15.8K | 63.0 | 156 | 186 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.8K | 82.4 | 185 | 194 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 81.3K | 12.4 | 27.9 | 34.3 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 86.8K | 10.0 | 30.1 | 38.1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 29.0K | 9.92 | 389 | 569 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 18.4K | 6.75 | 934 | 1237 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2370577 | 1 | 220.4K | 4.42 | 9.66 | 12.4 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2390768 | 1 | 214.1K | 3.23 | 12.2 | 14.8 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 26.1K | 11.8 | 299 | 328 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 14.9K | 17.2 | 594 | 651 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 5373279 | 4 | 535.6K | 6.46 | 13.4 | 58.9 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6786577 | 4 | 645.4K | 6.33 | 10.4 | 13.6 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 16000000 | 16 | 1.51M | 162 | 223 | 1065 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 24000000 | 16 | 2.34M | 84.5 | 175 | 532 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.3K | 49.9 | 116 | 151 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 631864 | 4 | 60.4K | 51.7 | 148 | 178 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 2165645 | 16 | 216.5K | 57.9 | 166 | 395 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.3K | 63.0 | 81.9 | 105 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 291798 | 4 | 28.4K | 143 | 168 | 202 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 1073346 | 16 | 98.3K | 172 | 224 | 291 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2388450 | 32 | 221.7K | 115 | 221 | 4145 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6614467 | 32 | 567.7K | 40.4 | 138 | 528 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1000000 | 6922675 | 32 | 560.8K | 40.4 | 137 | 672 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6365083 | 32 | 570.8K | 51.2 | 101 | 344 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1664 MiB | 0 | 166274 | 9.21 | 18.2 | 56.6 | 804247 | 5.38 | 15.7 | 25.1 |
| ycsb-a | rocksdb | 1257 MiB | 0 | 110227 | 10.8 | 16.4 | 18.9 | 393164 | 18.7 | 32.4 | 40.7 |
| ycsb-b | pigeonhole | 1664 MiB | 0 | 277949 | 8.51 | 14.3 | 17.4 | 2687666 | 0.94 | 12.8 | 15.8 |
| ycsb-b | rocksdb | 1211 MiB | 0 | 158422 | 8.96 | 12.8 | 15.4 | 791650 | 9.66 | 22.7 | 25.9 |
| ycsb-c | pigeonhole | 1984 MiB | 0 | 311085 | 7.62 | 13.2 | 16.3 | 3773943 | 0.84 | 11.5 | 14.4 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 212822 | 8.26 | 12.0 | 14.4 | 1428938 | 4.51 | 11.0 | 13.8 |
| ycsb-d | pigeonhole | 1856 MiB | 0 | 496084 | 8.26 | 14.8 | 30.7 | 2297471 | 0.94 | 11.4 | 16.0 |
| ycsb-d | rocksdb | 1286 MiB | 0 | 272541 | 10.5 | 16.1 | 18.7 | 914472 | 6.01 | 15.4 | 17.9 |
| ycsb-f | pigeonhole | 1664 MiB | 0 | 110217 | 10.1 | 17.7 | 25.2 | 389488 | 5.34 | 15.9 | 21.6 |
| ycsb-f | rocksdb | 1256 MiB | 0 | 110217 | 10.4 | 16.2 | 18.8 | 389488 | 19.1 | 33.3 | 41.5 |
| sparse-wide | pigeonhole | 2432 MiB | 0 | 122676 | 3.55 | 16.1 | 19.7 | 477384 | 3.57 | 365 | 524 |
| sparse-wide | rocksdb | 2123 MiB | 0 | 122676 | 6.69 | 15.6 | 18.4 | 477384 | 5.76 | 926 | 1204 |
| time-series-ttl | pigeonhole | 448 MiB | 0 | 387911 | 1.50 | 4.38 | 6.27 | 85307 | 1.32 | 4.16 | 5.54 |
| time-series-ttl | rocksdb | 254 MiB | 0 | 391111 | 1.13 | 5.09 | 7.01 | 86100 | 1.02 | 4.29 | 6.30 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 971120 | 4.99 | 4.58 | 8.70 | 11.8 |
| ycsb-a | pigeonhole | row read | 970521 | 6.03 | 6.17 | 16.3 | 27.5 |
| ycsb-a | rocksdb | put | 503758 | 3.33 | 3.15 | 5.86 | 8.26 |
| ycsb-a | rocksdb | row read | 503391 | 16.5 | 14.3 | 31.7 | 39.7 |
| ycsb-b | pigeonhole | put | 156076 | 8.36 | 8.89 | 14.1 | 16.8 |
| ycsb-b | pigeonhole | row read | 2965615 | 3.20 | 1.01 | 13.3 | 15.9 |
| ycsb-b | rocksdb | put | 49928 | 3.88 | 3.58 | 7.10 | 9.28 |
| ycsb-b | rocksdb | row read | 950072 | 11.4 | 9.41 | 22.5 | 25.7 |
| ycsb-d | pigeonhole | put | 146867 | 14.2 | 13.0 | 19.8 | 24.2 |
| ycsb-d | pigeonhole | row read | 2793555 | 2.97 | 1.09 | 13.2 | 17.2 |
| ycsb-d | rocksdb | put | 62476 | 7.50 | 7.29 | 11.5 | 13.8 |
| ycsb-d | rocksdb | row read | 1187013 | 8.35 | 8.19 | 15.6 | 18.2 |
| ycsb-e | pigeonhole | put | 49830 | 16.1 | 14.5 | 19.5 | 30.2 |
| ycsb-e | pigeonhole | scan | 950170 | 65.9 | 65.3 | 157 | 187 |
| ycsb-e | rocksdb | put | 49830 | 9.53 | 9.41 | 14.1 | 17.2 |
| ycsb-e | rocksdb | scan | 950170 | 88.8 | 86.5 | 186 | 194 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 18.1 | 17.4 | 29.6 | 36.6 |
| ycsb-f | pigeonhole | row read | 499705 | 6.35 | 6.27 | 16.6 | 22.4 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.43 | 4.80 | 14.3 | 17.4 |
| ycsb-f | rocksdb | row read | 499705 | 16.5 | 14.1 | 32.4 | 40.7 |
| sparse-wide | pigeonhole | get | 399998 | 3.43 | 2.75 | 11.1 | 14.1 |
| sparse-wide | pigeonhole | put | 200020 | 11.0 | 10.7 | 16.6 | 20.9 |
| sparse-wide | pigeonhole | row read | 200062 | 57.9 | 14.5 | 461 | 532 |
| sparse-wide | pigeonhole | scan | 199920 | 96.6 | 68.1 | 520 | 598 |
| sparse-wide | rocksdb | get | 399998 | 4.21 | 4.48 | 9.79 | 12.0 |
| sparse-wide | rocksdb | put | 200020 | 5.14 | 4.83 | 9.79 | 12.1 |
| sparse-wide | rocksdb | row read | 200062 | 113 | 15.2 | 1073 | 1237 |
| sparse-wide | rocksdb | scan | 199920 | 144 | 47.9 | 1130 | 1294 |
| time-series-ttl | pigeonhole | get | 473218 | 1.79 | 1.49 | 4.35 | 6.21 |
| time-series-ttl | pigeonhole | put at | 949005 | 5.50 | 4.67 | 10.4 | 12.8 |
| time-series-ttl | pigeonhole | scan | 948354 | 4.83 | 4.58 | 8.96 | 12.7 |
| time-series-ttl | rocksdb | get | 477211 | 1.66 | 1.10 | 4.96 | 6.94 |
| time-series-ttl | rocksdb | put at | 957181 | 3.07 | 2.94 | 5.12 | 7.90 |
| time-series-ttl | rocksdb | scan | 956376 | 7.67 | 7.17 | 13.1 | 16.3 |
| adjacency | pigeonhole | put | 99878 | 10.2 | 10.2 | 14.4 | 17.3 |
| adjacency | pigeonhole | scan | 900122 | 41.3 | 13.8 | 301 | 328 |
| adjacency | rocksdb | put | 99878 | 4.55 | 4.42 | 7.81 | 10.2 |
| adjacency | rocksdb | scan | 900122 | 73.9 | 21.2 | 598 | 651 |
| cross-shard-contention | pigeonhole | cross-shard commit | 1194141 | 146 | 115 | 223 | 4719 |
| cross-shard-contention | pigeonhole | put | 1194309 | 139 | 115 | 218 | 3834 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3308328 | 58.7 | 42.8 | 142 | 561 |
| cross-shard-contention | pigeonhole | put | 3306139 | 52.0 | 37.9 | 135 | 487 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3462530 | 59.7 | 42.5 | 140 | 725 |
| cross-shard-contention | pigeonhole | put | 3460145 | 53.6 | 37.9 | 133 | 610 |
| cross-shard-contention | rocksdb | cross-shard commit | 3183913 | 57.9 | 53.0 | 103 | 352 |
| cross-shard-contention | rocksdb | put | 3181170 | 53.2 | 49.4 | 96.8 | 340 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2388450 | 1194141 | 9976 | 0.008 | 10044, 0, 94, 92, 0 | 2398426 | 9976 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6614467 | 3308328 | 43134 | 0.013 | 46638, 0, 0, 0, 0 | 6657601 | 43134 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6922675 | 3462530 | 42846 | 0.012 | 46022, 0, 0, 0, 0 | 6965521 | 42846 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 971120 | 100.0% | 11 → 12 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 156076 | 100.0% | 9 → 9 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 5 → 5 | 0 | 1 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 4 → 3 | 0 | 0 | 1 |
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
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 93065 | 63.4% | 6 → 7 | 1 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 53802 | 36.6% | 3 → 3 | 0 | 0 | 0 |
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
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 49830 | 100.0% | 8 → 8 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 500295 | 100.0% | 7 → 7 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 200020 | 100.0% | 11 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 465806 | 49.1% | 10 → 10 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 483199 | 50.9% | 9 → 9 | 0 | 0 | 0 |
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
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 3009694 | 56.0% | 16 → 10 | 0 | 0 | 6 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1810302 | 33.7% | 0 → 4 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 553283 | 10.3% | 0 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 0 | 989872 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1 | 1005176 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 2 | 991832 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 3 | 1004968 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 4 | 993440 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 5 | 1005392 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 6 | 994568 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 7 | 1006784 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 8 | 989400 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 9 | 1009960 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 10 | 995712 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 11 | 1007864 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 12 | 993112 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 13 | 1012008 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 14 | 990648 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 15 | 1009264 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 0 | 1484808 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1 | 1507764 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 2 | 1487748 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 3 | 1507452 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 4 | 1490160 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 5 | 1508088 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 6 | 1491852 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 7 | 1510176 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 8 | 1484100 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 9 | 1514940 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 10 | 1493568 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 11 | 1511796 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 12 | 1489668 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 13 | 1518012 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 14 | 1485972 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 15 | 1513896 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 10422 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 8320 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 17598 | 8.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 6396 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 15342 | 7.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 8682 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 9779 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 4016 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 18096 | 9.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 18943 | 9.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 18385 | 9.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 8687 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 15833 | 7.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 7835 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 7883 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 23783 | 11.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 104741 | 16.6% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 84771 | 13.4% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 84561 | 13.4% | 1 → 3 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 196986 | 31.2% | 1 → 7 | 0 | 0 | 5 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 79096 | 12.5% | 1 → 3 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 2074 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 10119 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 7348 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 8928 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 16975 | 2.7% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 2411 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 7703 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 6279 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 6010 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 7811 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 6051 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 157432 | 7.3% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 84182 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 163594 | 7.6% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 173462 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 104141 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 113177 | 5.2% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 144565 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 158266 | 7.3% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 108682 | 5.0% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 162522 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 136138 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 91094 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 97464 | 4.5% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 147643 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 160670 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 162613 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1631303 | 68.3% | 16 → 7 | 0 | 0 | 9 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 603458 | 25.3% | 1 → 6 | 0 | 0 | 1 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 107122 | 4.5% | 0 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 46567 | 1.9% | 0 → 3 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 414134 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 414628 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 412287 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 414289 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 412936 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 413997 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 413612 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 412367 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 412951 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 413231 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 412506 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 413426 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 414360 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 412496 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 413012 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 414235 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 0 | 433361 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1 | 433926 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 2 | 431441 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 3 | 433639 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 4 | 432079 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 5 | 433067 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6 | 432650 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7 | 431713 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 8 | 432262 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 9 | 432435 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 10 | 432044 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 11 | 432754 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 12 | 433714 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 13 | 431888 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 14 | 432146 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 15 | 433556 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 3 | 73.3 | 10 | 6 | 2 (16) | 0 | 0 | 0 | 0.0 | 0.01 | 0.01 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.04 | 0.04 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 2 | 76.1 | 17 | 9 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 1 | 51.1 | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 | 0.0 | 3 | 2 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.01 | 0.01 | 0.50 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 1 | 31.9 | 3 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.17 | 0.17 | 0.00 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 2 | 103.7 | 4 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 9 | 271.5 | 55 | 29 | 5 (34) | 0 | 0 | 0 | 0.0 | 0.00 | 0.03 | 0.00 | 290 (0) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 648 | 160 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.12 | 0.00 | 76145 (6070) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 976 | 240 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.11 | 0.00 | 1888433 (427575) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 1 | 1 | 0 (0) | 0 | 0 | 1 | 0.1 | 2.00 | 2.00 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 1 | 8.7 | 32 | 2 | 0 (0) | 0 | 0 | 6 | 3.1 | 0.94 | 1.30 | 1.00 | 1307 (8) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 87 | 24 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.69 | 1.11 | 1.00 | 46055 (5843) | 0 |
| cross-shard-contention | pigeonhole | 32 | 28 | 1137.8 | 66 | 60 | 27 (23) | 0 | 0 | 0 | 0.0 | 0.03 | 0.04 | 0.98 | 1710210 (157722) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 4 | 1.6 | 0.04 | 0.18 | 0.12 | 773404 (481130) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 3 | 1.9 | 0.04 | 0.16 | 0.11 | 786178 (499569) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 6.05M cells/s (377.8K per core), commit p50 162 µs, p99 223 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 131 µs, p99 197 µs, p99.9 786 µs; 0 WAL rollover blocks, 0 inline WAL syncs — fail.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 9.36M cells/s, commit p50 84.5 µs, p99 175 µs.
