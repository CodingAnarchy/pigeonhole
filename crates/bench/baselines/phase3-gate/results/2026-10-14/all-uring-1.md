**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 0.95 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1867248 | 1 | 180.8K | 4.42 | 14.0 | 25.9 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1008426 | 1 | 100.5K | 6.40 | 28.9 | 36.1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3086155 | 1 | 280.5K | 1.05 | 13.4 | 16.4 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 89.3K | 9.34 | 22.8 | 26.2 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4018418 | 1 | 381.9K | 0.92 | 12.2 | 14.9 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1632969 | 1 | 157.5K | 5.54 | 11.1 | 14.0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2899562 | 1 | 257.9K | 1.12 | 14.4 | 74.8 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1265913 | 1 | 120.8K | 7.87 | 15.4 | 17.8 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 13.5K | 71.2 | 185 | 228 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.7K | 82.9 | 186 | 194 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 86.9K | 10.5 | 28.4 | 35.6 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 85.5K | 10.4 | 30.3 | 38.1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 27.0K | 10.6 | 446 | 582 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 18.2K | 6.88 | 950 | 1270 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2108672 | 1 | 210.8K | 4.19 | 11.5 | 15.9 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2364948 | 1 | 212.8K | 3.25 | 12.2 | 14.7 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 24.6K | 13.4 | 324 | 358 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 14.9K | 17.4 | 594 | 651 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4538790 | 4 | 453.7K | 6.75 | 13.6 | 518 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6910122 | 4 | 645.1K | 6.30 | 10.8 | 13.8 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 24000000 | 16 | 2.38M | 85.5 | 145 | 819 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 26000000 | 16 | 2.41M | 82.9 | 170 | 504 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.4K | 49.7 | 118 | 147 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 633483 | 4 | 59.4K | 53.2 | 148 | 193 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 1924694 | 16 | 189.6K | 58.4 | 183 | 3015 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.3K | 63.0 | 81.9 | 105 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 291974 | 4 | 28.4K | 142 | 168 | 200 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 1075639 | 16 | 97.8K | 171 | 222 | 285 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2023637 | 32 | 184.2K | 116 | 1106 | 2703 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 4907390 | 32 | 384.3K | 49.2 | 963 | 2982 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6441465 | 32 | 569.3K | 50.9 | 104 | 317 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1600 MiB | 0 | 162597 | 8.96 | 17.3 | 25.1 | 770603 | 5.09 | 14.3 | 21.1 |
| ycsb-a | rocksdb | 1257 MiB | 0 | 110314 | 10.9 | 16.5 | 19.3 | 393718 | 18.7 | 31.2 | 40.4 |
| ycsb-b | pigeonhole | 1600 MiB | 0 | 276670 | 8.38 | 14.3 | 17.9 | 2655161 | 0.94 | 12.9 | 16.1 |
| ycsb-b | rocksdb | 1211 MiB | 0 | 158422 | 9.02 | 13.0 | 15.4 | 791650 | 9.79 | 23.3 | 26.4 |
| ycsb-c | pigeonhole | 1536 MiB | 0 | 309407 | 7.65 | 13.4 | 17.0 | 3709011 | 0.83 | 11.7 | 14.7 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 212257 | 8.32 | 12.0 | 14.5 | 1420712 | 4.54 | 11.1 | 13.8 |
| ycsb-d | pigeonhole | 1728 MiB | 0 | 491474 | 7.84 | 14.5 | 18.4 | 2263342 | 0.91 | 11.1 | 15.3 |
| ycsb-d | rocksdb | 1288 MiB | 0 | 275202 | 10.4 | 15.9 | 18.4 | 927397 | 5.89 | 15.2 | 17.7 |
| ycsb-f | pigeonhole | 1600 MiB | 0 | 110217 | 9.28 | 16.4 | 20.7 | 389488 | 4.96 | 14.2 | 18.7 |
| ycsb-f | rocksdb | 1254 MiB | 0 | 110217 | 10.8 | 16.3 | 18.9 | 389488 | 19.1 | 33.3 | 41.2 |
| sparse-wide | pigeonhole | 2432 MiB | 0 | 122676 | 3.77 | 16.9 | 20.5 | 477384 | 3.47 | 444 | 569 |
| sparse-wide | rocksdb | 2123 MiB | 0 | 122676 | 6.82 | 15.7 | 18.4 | 477384 | 5.89 | 934 | 1245 |
| time-series-ttl | pigeonhole | 768 MiB | 0 | 345853 | 1.42 | 4.25 | 6.85 | 75107 | 1.22 | 3.79 | 5.92 |
| time-series-ttl | rocksdb | 253 MiB | 0 | 387012 | 1.14 | 5.12 | 7.13 | 85083 | 1.04 | 4.35 | 6.01 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 934048 | 5.41 | 4.08 | 10.2 | 494 |
| ycsb-a | pigeonhole | row read | 933200 | 5.56 | 5.76 | 15.2 | 22.5 |
| ycsb-a | rocksdb | put | 504394 | 3.33 | 3.15 | 5.89 | 8.32 |
| ycsb-a | rocksdb | row read | 504032 | 16.5 | 14.3 | 30.6 | 39.4 |
| ycsb-b | pigeonhole | put | 154324 | 9.50 | 9.60 | 14.1 | 17.7 |
| ycsb-b | pigeonhole | row read | 2931831 | 3.19 | 1.01 | 13.3 | 16.3 |
| ycsb-b | rocksdb | put | 49928 | 3.92 | 3.63 | 7.17 | 9.34 |
| ycsb-b | rocksdb | row read | 950072 | 11.5 | 9.47 | 22.9 | 26.2 |
| ycsb-d | pigeonhole | put | 144746 | 21.4 | 12.7 | 426 | 520 |
| ycsb-d | pigeonhole | row read | 2754816 | 2.89 | 1.06 | 12.8 | 16.1 |
| ycsb-d | rocksdb | put | 63314 | 7.48 | 7.29 | 11.4 | 13.8 |
| ycsb-d | rocksdb | row read | 1202599 | 8.25 | 8.06 | 15.4 | 17.9 |
| ycsb-e | pigeonhole | put | 49830 | 33.8 | 36.9 | 46.8 | 67.6 |
| ycsb-e | pigeonhole | scan | 950170 | 76.1 | 73.7 | 186 | 228 |
| ycsb-e | rocksdb | put | 49830 | 9.59 | 9.54 | 14.0 | 16.5 |
| ycsb-e | rocksdb | scan | 950170 | 89.3 | 87.0 | 186 | 195 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 17.1 | 16.0 | 30.1 | 494 |
| ycsb-f | pigeonhole | row read | 499705 | 5.69 | 5.79 | 15.2 | 19.3 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.54 | 4.80 | 14.4 | 17.5 |
| ycsb-f | rocksdb | row read | 499705 | 16.7 | 14.3 | 32.5 | 40.4 |
| sparse-wide | pigeonhole | get | 399998 | 3.41 | 2.75 | 11.5 | 15.2 |
| sparse-wide | pigeonhole | put | 200020 | 11.7 | 11.5 | 17.9 | 52.0 |
| sparse-wide | pigeonhole | row read | 200062 | 65.5 | 15.0 | 522 | 582 |
| sparse-wide | pigeonhole | scan | 199920 | 101 | 68.1 | 541 | 606 |
| sparse-wide | rocksdb | get | 399998 | 4.28 | 4.58 | 9.73 | 11.9 |
| sparse-wide | rocksdb | put | 200020 | 5.19 | 4.86 | 9.98 | 12.2 |
| sparse-wide | rocksdb | row read | 200062 | 115 | 15.4 | 1090 | 1253 |
| sparse-wide | rocksdb | scan | 199920 | 146 | 48.4 | 1155 | 1311 |
| time-series-ttl | pigeonhole | get | 420960 | 1.71 | 1.40 | 4.16 | 6.75 |
| time-series-ttl | pigeonhole | put at | 844108 | 6.46 | 4.67 | 12.7 | 33.3 |
| time-series-ttl | pigeonhole | scan | 843604 | 4.42 | 4.09 | 8.77 | 13.1 |
| time-series-ttl | rocksdb | get | 472095 | 1.67 | 1.12 | 4.99 | 7.04 |
| time-series-ttl | rocksdb | put at | 946774 | 3.07 | 2.94 | 5.15 | 7.97 |
| time-series-ttl | rocksdb | scan | 946079 | 7.72 | 7.20 | 13.0 | 16.1 |
| adjacency | pigeonhole | put | 99878 | 15.4 | 10.9 | 42.5 | 477 |
| adjacency | pigeonhole | scan | 900122 | 43.4 | 14.7 | 326 | 358 |
| adjacency | rocksdb | put | 99878 | 4.59 | 4.48 | 7.84 | 10.1 |
| adjacency | rocksdb | scan | 900122 | 74.2 | 21.6 | 598 | 651 |
| cross-shard-contention | pigeonhole | cross-shard commit | 1011649 | 177 | 116 | 1106 | 3129 |
| cross-shard-contention | pigeonhole | put | 1011988 | 169 | 115 | 1106 | 2179 |
| cross-shard-contention | pigeonhole | cross-shard commit | 2453877 | 86.2 | 50.9 | 995 | 3523 |
| cross-shard-contention | pigeonhole | put | 2453513 | 79.9 | 47.1 | 889 | 2474 |
| cross-shard-contention | rocksdb | cross-shard commit | 3222153 | 57.7 | 52.7 | 106 | 324 |
| cross-shard-contention | rocksdb | put | 3219312 | 53.2 | 49.4 | 101 | 315 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2023637 | 1011649 | 3986 | 0.004 | 3892, 0, 18, 76, 0 | 2027623 | 3986 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4907390 | 2453877 | 21507 | 0.009 | 22640, 0, 0, 0, 0 | 4928897 | 21507 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 747821 | 80.1% | 11 → 16 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 186227 | 19.9% | 5 → 0 | 0 | 0 | 5 |
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
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 79299 | 51.4% | 6 → 6 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 75025 | 48.6% | 10 → 10 | 0 | 0 | 0 |
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
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 11 → 11 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 5 → 5 | 0 | 0 | 0 |
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
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 66074 | 45.6% | 5 → 5 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 78672 | 54.4% | 12 → 12 | 0 | 0 | 0 |
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
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 25162 | 50.5% | 11 → 11 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 24668 | 49.5% | 5 → 5 | 0 | 0 | 0 |
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
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 405861 | 81.1% | 6 → 15 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 94434 | 18.9% | 10 → 1 | 0 | 0 | 9 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 73528 | 36.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 126383 | 63.2% | 9 → 9 | 0 | 0 | 0 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 109 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 102726 | 12.2% | 1 → 6 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 574820 | 68.1% | 0 → 14 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 6725 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 13926 | 1.6% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 15797 | 1.9% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 5982 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 11872 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 21132 | 2.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 8992 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 10119 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 12587 | 1.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 12636 | 1.5% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 6881 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 10642 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 9045 | 1.1% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 20226 | 2.4% | 2 → 0 | 0 | 0 | 2 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 172 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 87008 | 87.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 892 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 334 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 255 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 220 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 103 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1342 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 462 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 253 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 133 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 94 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 327 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 175 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 2055 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 6053 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2675467 | 58.9% | 16 → 10 | 0 | 0 | 6 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1863323 | 41.1% | 0 → 6 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 0 | 1484808 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1 | 1507764 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 2 | 1487748 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 3 | 1507452 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 4 | 1490160 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 5 | 1508088 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 6 | 1491852 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 7 | 1510176 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 8 | 1484100 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 9 | 1514940 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 10 | 1493568 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 11 | 1511796 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 12 | 1489668 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 13 | 1518012 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 14 | 1485972 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 15 | 1513896 | 6.3% | 1 → 1 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 16190 | 8.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 12518 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 22553 | 11.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 15301 | 7.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 8009 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 9955 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 7580 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 14389 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 14107 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 10768 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 10213 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 9037 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 20945 | 10.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 5881 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 18618 | 9.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 3936 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 227473 | 35.9% | 1 → 7 | 0 | 0 | 5 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 67626 | 10.7% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 76120 | 12.0% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 78160 | 12.3% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 83242 | 13.1% | 1 → 3 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 9744 | 1.5% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 12787 | 2.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 11626 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 8085 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 16422 | 2.6% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 4869 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 12790 | 2.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 4300 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 11887 | 1.9% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 1430 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 6922 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 153939 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 141868 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 129891 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 125214 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 172115 | 8.9% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 159712 | 8.3% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 120916 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 96496 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 63720 | 3.3% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 134801 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 99973 | 5.2% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 123225 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 111653 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 88786 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 92529 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 109856 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1771900 | 87.6% | 16 → 16 | 5 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 251737 | 12.4% | 1 → 6 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 307447 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 307441 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 305576 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 307694 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 305897 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 307246 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 307537 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 305909 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 306318 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 306411 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 306006 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 306869 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 307660 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 305393 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 306316 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 307670 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 3 | 102.9 | 13 | 3 | 1 (4) | 0 | 0 | 0 | 0.0 | 0.04 | 0.04 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 3 | 117.1 | 14 | 7 | 1 (3) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.05 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 1 | 54.1 | 10 | 3 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.13 | 0.13 | 0.50 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.19 | 0.19 | 0.00 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 1 | 35.3 | 21 | 18 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.12 | 0.12 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 1 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.02 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 12 | 317.8 | 46 | 29 | 6 (35) | 0 | 0 | 0 | 0.0 | 0.00 | 0.03 | 0.01 | 139 (1) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 970 | 240 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.11 | 0.00 | 150710 (25277) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 1062 | 264 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.11 | 0.00 | 1616818 (353415) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 2 | 2 | 0 (0) | 0 | 0 | 1 | 0.1 | 2.00 | 1.99 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 2 | 17.9 | 34 | 15 | 1 (4) | 0 | 0 | 5 | 0.4 | 0.91 | 1.25 | 1.00 | 1121 (7) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 79 | 19 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.75 | 1.16 | 1.00 | 64236 (6656) | 0 |
| cross-shard-contention | pigeonhole | 32 | 31 | 940.1 | 54 | 94 | 17 (31) | 0 | 0 | 0 | 0.0 | 0.04 | 0.04 | 1.00 | 1604866 (66873) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 96 | 32 | 0 (0) | 0 | 0 | 1 | 11.2 | 0.05 | 0.21 | 0.98 | 3496227 (3475679) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 9.53M cells/s (595.4K per core), commit p50 85.5 µs, p99 145 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 65.5 µs, p99 131 µs, p99.9 786 µs; 0 WAL rollover blocks, 0 inline WAL syncs — pass.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 9.66M cells/s, commit p50 82.9 µs, p99 170 µs.
