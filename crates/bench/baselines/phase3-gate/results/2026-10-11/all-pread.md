**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.10 [non-reference (D5)]
Build: release, rev f333d4879d6d. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 132.1K | 7.62 | 14.5 | 22.4 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 91.8K | 6.43 | 38.1 | 43.5 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 180.0K | 4.29 | 14.8 | 34.3 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 53.0K | 17.5 | 37.1 | 40.2 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 214.3K | 3.50 | 13.1 | 15.7 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 79.6K | 13.4 | 21.9 | 24.8 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 250.0K | 2.45 | 15.8 | 37.1 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 107.3K | 8.38 | 22.3 | 26.2 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 16.3K | 59.4 | 140 | 162 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.5K | 84.0 | 190 | 203 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 101.9K | 9.73 | 20.6 | 33.5 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 77.5K | 10.6 | 38.7 | 47.1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 31.5K | 9.28 | 428 | 586 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 17.2K | 7.17 | 1024 | 1532 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 184.1K | 5.28 | 10.9 | 14.2 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 252.3K | 3.17 | 9.34 | 11.8 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 25.6K | 13.9 | 311 | 344 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 17.3K | 14.6 | 532 | 586 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 4 | 398.3K | 8.10 | 16.1 | 34.6 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2000000 | 4 | 559.4K | 6.21 | 11.0 | 14.5 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 1000 | 200000 | 32 | 733.5K | 154 | 287 | 358 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 18.6K | 46.6 | 116 | 139 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 4 | 54.1K | 57.1 | 133 | 158 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 16 | 112.4K | 119 | 255 | 553 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.4K | 63.0 | 77.8 | 100 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 4 | 27.8K | 143 | 170 | 202 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 16 | 92.2K | 181 | 249 | 285 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1856 MiB | 0 | 109752 | 9.09 | 17.7 | 25.1 | 390163 | 4.89 | 13.6 | 17.7 |
| ycsb-a | rocksdb | 1264 MiB | 0 | 109752 | 13.0 | 25.7 | 30.1 | 390163 | 19.6 | 40.2 | 47.9 |
| ycsb-b | pigeonhole | 1664 MiB | 0 | 158422 | 8.57 | 14.9 | 18.9 | 791650 | 3.87 | 12.2 | 15.6 |
| ycsb-b | rocksdb | 1214 MiB | 0 | 158422 | 17.4 | 23.3 | 26.8 | 791650 | 18.0 | 37.4 | 40.4 |
| ycsb-c | pigeonhole | 1648 MiB | 0 | 162648 | 8.26 | 14.5 | 18.0 | 837352 | 3.06 | 11.5 | 15.0 |
| ycsb-c | rocksdb | 1205 MiB | 0 | 162648 | 17.2 | 23.0 | 26.6 | 837352 | 9.41 | 21.6 | 24.4 |
| ycsb-d | pigeonhole | 1680 MiB | 0 | 230659 | 4.64 | 15.6 | 23.6 | 719316 | 2.04 | 8.83 | 12.5 |
| ycsb-d | rocksdb | 1271 MiB | 0 | 230659 | 11.5 | 23.7 | 27.8 | 719316 | 7.10 | 21.6 | 25.5 |
| ycsb-f | pigeonhole | 1680 MiB | 0 | 110217 | 8.89 | 15.9 | 20.4 | 389488 | 4.96 | 14.0 | 17.8 |
| ycsb-f | rocksdb | 1263 MiB | 0 | 110217 | 12.6 | 24.8 | 29.2 | 389488 | 20.0 | 41.2 | 53.0 |
| sparse-wide | pigeonhole | 2352 MiB | 0 | 122676 | 3.31 | 15.7 | 20.1 | 477384 | 3.21 | 408 | 528 |
| sparse-wide | rocksdb | 2118 MiB | 0 | 122676 | 7.29 | 16.4 | 18.9 | 477384 | 6.37 | 1040 | 1581 |
| time-series-ttl | pigeonhole | 272 MiB | 0 | 166812 | 1.25 | 3.57 | 5.44 | 32311 | 1.09 | 3.09 | 5.09 |
| time-series-ttl | rocksdb | 182 MiB | 0 | 166812 | 1.09 | 5.18 | 7.10 | 32311 | 0.94 | 3.94 | 5.54 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 8.74 | 8.16 | 13.1 | 24.3 |
| ycsb-a | pigeonhole | row read | 499915 | 6.32 | 5.50 | 15.2 | 21.5 |
| ycsb-a | rocksdb | put | 500085 | 3.42 | 3.23 | 6.08 | 8.51 |
| ycsb-a | rocksdb | row read | 499915 | 18.3 | 18.0 | 39.7 | 46.3 |
| ycsb-b | pigeonhole | put | 49928 | 12.2 | 9.66 | 35.3 | 42.2 |
| ycsb-b | pigeonhole | row read | 950072 | 5.16 | 4.16 | 13.5 | 16.3 |
| ycsb-b | rocksdb | put | 49928 | 4.90 | 4.67 | 8.38 | 10.9 |
| ycsb-b | rocksdb | row read | 950072 | 19.6 | 17.8 | 37.1 | 40.2 |
| ycsb-d | pigeonhole | put | 50025 | 14.8 | 12.5 | 39.7 | 397 |
| ycsb-d | pigeonhole | row read | 949975 | 3.39 | 2.33 | 12.9 | 17.5 |
| ycsb-d | rocksdb | put | 50025 | 7.74 | 7.52 | 11.9 | 14.5 |
| ycsb-d | rocksdb | row read | 949975 | 9.36 | 8.57 | 22.4 | 26.4 |
| ycsb-e | pigeonhole | put | 49830 | 24.0 | 22.1 | 42.0 | 47.9 |
| ycsb-e | pigeonhole | scan | 950170 | 63.4 | 61.7 | 141 | 162 |
| ycsb-e | rocksdb | put | 49830 | 9.74 | 9.66 | 14.3 | 16.8 |
| ycsb-e | rocksdb | scan | 950170 | 90.6 | 88.1 | 190 | 203 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 13.1 | 12.2 | 22.3 | 40.7 |
| ycsb-f | pigeonhole | row read | 499705 | 6.46 | 5.66 | 14.8 | 18.7 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 7.35 | 4.96 | 21.4 | 25.7 |
| ycsb-f | rocksdb | row read | 499705 | 18.4 | 17.9 | 40.4 | 51.7 |
| sparse-wide | pigeonhole | get | 399998 | 3.00 | 2.32 | 10.4 | 13.9 |
| sparse-wide | pigeonhole | put | 200020 | 10.6 | 9.92 | 23.6 | 38.4 |
| sparse-wide | pigeonhole | row read | 200062 | 58.0 | 12.9 | 485 | 537 |
| sparse-wide | pigeonhole | scan | 199920 | 83.7 | 38.9 | 549 | 606 |
| sparse-wide | rocksdb | get | 399998 | 4.58 | 4.89 | 10.7 | 12.6 |
| sparse-wide | rocksdb | put | 200020 | 5.17 | 4.80 | 10.2 | 12.5 |
| sparse-wide | rocksdb | row read | 200062 | 123 | 15.6 | 1343 | 1827 |
| sparse-wide | rocksdb | scan | 199920 | 152 | 48.4 | 1368 | 1638 |
| time-series-ttl | pigeonhole | get | 199123 | 1.42 | 1.22 | 3.50 | 5.41 |
| time-series-ttl | pigeonhole | put at | 400947 | 7.79 | 7.58 | 12.2 | 15.9 |
| time-series-ttl | pigeonhole | scan | 399930 | 4.96 | 4.67 | 8.51 | 10.9 |
| time-series-ttl | rocksdb | get | 199123 | 1.60 | 1.05 | 5.09 | 6.91 |
| time-series-ttl | rocksdb | put at | 400947 | 3.04 | 2.91 | 5.05 | 8.00 |
| time-series-ttl | rocksdb | scan | 399930 | 5.97 | 5.63 | 10.4 | 12.5 |
| adjacency | pigeonhole | put | 99878 | 17.5 | 12.7 | 38.9 | 50.2 |
| adjacency | pigeonhole | scan | 900122 | 41.4 | 14.3 | 313 | 344 |
| adjacency | rocksdb | put | 99878 | 4.33 | 4.05 | 7.52 | 9.54 |
| adjacency | rocksdb | scan | 900122 | 63.7 | 18.4 | 537 | 590 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 12239 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 50805 | 10.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 5525 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 7993 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 16532 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 22606 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 15162 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 13044 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 10269 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 8543 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 11027 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 12221 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 4795 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 25420 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 6661 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 17536 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 31849 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 7887 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 7476 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 12083 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 10664 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 17786 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 14595 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 11213 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 7350 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 32255 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 10044 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 15553 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 5495 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 8992 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 40646 | 8.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 25819 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 4249 | 8.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 832 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1458 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 456 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1223 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 4918 | 9.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1593 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 748 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 251 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2184 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 2624 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1599 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 543 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1260 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 2793 | 5.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2191 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 476 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 375 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 290 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 922 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 485 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 2512 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 1301 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1651 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 548 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 3335 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 767 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1607 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 186 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 2310 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 1269 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 2972 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1533 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 989 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1061 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 425 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1282 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 636 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1816 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1782 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 640 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 436 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 819 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 819 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1307 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1809 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 833 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 330 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 1514 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 2308 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 3162 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 451 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 5856 | 11.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 2101 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 1406 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 835 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 1446 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 4241 | 8.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 992 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1276 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 2097 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 1068 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 2018 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 2737 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2946 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 3676 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 710 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 989 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1424 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1224 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 177 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 3981 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 3244 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 248 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 686 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1036 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1535 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 869 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 961 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2527 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 869 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1336 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 1434 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 2262 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 1120 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 1668 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 1673 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1928 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 676 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 892 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 1798 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 357 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 366 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 1855 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 2756 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 2607 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 10478 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 80812 | 16.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2357 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 19901 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 10641 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 8657 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 19834 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 3297 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 8505 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 21394 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 79851 | 16.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 23573 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 3465 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 14972 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 6885 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 10125 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 8809 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 12491 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 8278 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 10926 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 2837 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 21473 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 4257 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 3024 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 8076 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 2097 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 13018 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 36617 | 7.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 10938 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 12518 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 4281 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 15908 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 71553 | 35.8% | 7 → 7 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 43970 | 22.0% | 4 → 4 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 24302 | 12.1% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 801 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 15631 | 7.8% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 33508 | 16.8% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 10255 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 14120 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 19477 | 4.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 4288 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1483 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 5278 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 11096 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 4218 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 5534 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 11833 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 8208 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 8954 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 18110 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 25475 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 14883 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 48040 | 12.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 27950 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 3648 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 10657 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 7178 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 25285 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 7150 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 23762 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 21413 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 6124 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 4586 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 6831 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 5506 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 5141 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 17190 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 15401 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 12128 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 14388 | 14.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 4827 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4914 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 5868 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 5797 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 9401 | 9.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 15636 | 15.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 6923 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 13399 | 13.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 4922 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 11260 | 11.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 47 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 88 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 909 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 86 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 211 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 27 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 72 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 160 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 10 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 14 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 32 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 70 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 132 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 82 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 94 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 149 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 17 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 24 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 84 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 146 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 89 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 56513 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 70140 | 3.5% | 0 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 66902 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 99399 | 5.0% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 42453 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 78802 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 61445 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 100299 | 5.0% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 55736 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 41904 | 2.1% | 1 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 93708 | 4.7% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 148001 | 7.4% | 2 → 2 | 3 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 29717 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 77745 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 72613 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 83505 | 4.2% | 1 → 3 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 95999 | 4.8% | 1 → 3 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 38449 | 1.9% | 1 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 40836 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 38331 | 1.9% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 69136 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 47131 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 76544 | 3.8% | 1 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 85062 | 4.3% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 32494 | 1.6% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 82948 | 4.1% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 31977 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 35291 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 26564 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 55248 | 2.8% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 21329 | 1.1% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 43779 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 0 | 6162 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 1 | 6117 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 2 | 6300 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 3 | 6377 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 4 | 6111 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 5 | 6206 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 6 | 6197 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 7 | 6467 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 8 | 6120 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 9 | 6266 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 10 | 6274 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 11 | 6471 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 12 | 6346 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 13 | 6163 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 14 | 6173 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 15 | 6355 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 16 | 6153 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 17 | 6344 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 18 | 6212 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 19 | 6407 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 20 | 5940 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 21 | 6238 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 22 | 6261 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 23 | 6306 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 24 | 6302 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 25 | 6253 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 26 | 6086 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 27 | 6251 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 28 | 6265 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 29 | 6154 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 30 | 6219 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 31 | 6504 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 1042 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 82213 | 41.1% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 9308 | 4.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 2159 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 6155 | 3.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 1338 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 2711 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 2310 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 4341 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 2312 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 2720 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 1404 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 7372 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 6845 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 2322 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 4842 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 2539 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 1228 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 6571 | 3.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 5419 | 2.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 4451 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 138 | 0.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 7953 | 4.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 2197 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 1722 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 1346 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 5153 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 4678 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 1995 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 5195 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 4717 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 5304 | 2.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 10337 | 5.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 52420 | 26.2% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 4645 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 3344 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 4464 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 2126 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 2730 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 12923 | 6.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 2788 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 4097 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 2147 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 6397 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 5301 | 2.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 4230 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 1229 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 4736 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 5009 | 2.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 3656 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 1068 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 3618 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 8998 | 4.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 4631 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 6605 | 3.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 3872 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 4330 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 3123 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 5113 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 6675 | 3.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 5129 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 2645 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 1805 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 9809 | 4.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 3639 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 114001 | 57.0% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 1266 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 746 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 3704 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 2620 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 2415 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 1613 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 3662 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 3522 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 456 | 0.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 3151 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 7833 | 3.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 2038 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 4289 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 1596 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 2470 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 3506 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 2691 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 856 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 1235 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 1359 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 4863 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 4151 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 708 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 927 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 1775 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 1601 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 5209 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 2591 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 4839 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 4668 | 2.3% | 0 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.50 | 0.49 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 | 0.0 | 10 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 4 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.07 | 0.05 | 0.04 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 | 0.0 | 5 | 2 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.49 | 0.49 | 0.00 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.20 | 0.20 | 0.01 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.39 | 0.39 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 0.10 | 0.05 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 0 | 0.0 | 21 | 0 | 0 (0) | 0 | 0 | 8 | 1.0 | 0.54 | 0.54 | 0.01 | 1026 (47) | 0 |
| group-commit | pigeonhole | 32 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.13 | 0.00 | 86641 (15787) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 4 | 32 | 0 (0) | 0 | 0 | 4 | 0.3 | 1.26 | 1.25 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 2 | 0.2 | 1.30 | 1.30 | 1.00 | 41188 (961) | 0 |
| group-commit | pigeonhole | 16 | 1 | 29.7 | 3 | 1 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.56 | 0.56 | 1.00 | 59463 (4426) | 0 |

**Durable group commit (D213):** 32 shards on 32 cores, the table split over every shard, 16 commits in flight per shard: 2.93M cells/s (91.7K per core), commit p50 154 µs, p99 287 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 3% of the commits; 0 tablet changes while measuring; inside the engine p50 98.3 µs, p99 197 µs, p99.9 262 µs; 0 WAL rollover blocks, 0 inline WAL syncs — fail.
