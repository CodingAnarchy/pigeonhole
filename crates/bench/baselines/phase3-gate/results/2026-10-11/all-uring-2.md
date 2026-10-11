**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.13 [non-reference (D5)]
Build: release, rev f333d4879d6d. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 115.6K | 9.41 | 16.0 | 24.7 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 91.3K | 6.43 | 38.1 | 44.8 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 181.6K | 4.25 | 15.4 | 33.3 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 52.3K | 17.5 | 36.9 | 40.2 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 217.1K | 3.49 | 13.0 | 15.7 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 79.5K | 13.4 | 21.9 | 25.0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 233.5K | 2.45 | 18.6 | 46.6 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 106.7K | 8.51 | 22.7 | 27.1 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 16.4K | 58.4 | 135 | 155 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.5K | 84.5 | 190 | 202 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 90.9K | 10.8 | 23.6 | 42.5 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 76.3K | 10.9 | 38.9 | 48.1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 30.4K | 11.1 | 416 | 582 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 17.3K | 7.20 | 1008 | 1524 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 158.4K | 4.70 | 15.6 | 43.5 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 253.8K | 3.13 | 9.41 | 11.8 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 25.4K | 14.0 | 315 | 348 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 17.5K | 14.6 | 514 | 573 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 4 | 307.7K | 11.1 | 21.8 | 270 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2000000 | 4 | 559.6K | 6.21 | 10.4 | 13.9 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 1000 | 200000 | 32 | 613.4K | 242 | 1401 | 3441 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.3K | 49.9 | 124 | 165 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 4 | 51.5K | 66.0 | 135 | 171 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 16 | 116.2K | 121 | 189 | 1036 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.4K | 63.2 | 77.8 | 100 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 4 | 27.9K | 143 | 170 | 202 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 16 | 93.8K | 179 | 247 | 328 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1872 MiB | 0 | 109752 | 9.09 | 18.4 | 26.4 | 390163 | 4.61 | 13.5 | 20.0 |
| ycsb-a | rocksdb | 1263 MiB | 0 | 109752 | 13.0 | 25.7 | 30.3 | 390163 | 19.5 | 41.0 | 48.4 |
| ycsb-b | pigeonhole | 1736 MiB | 0 | 158422 | 8.51 | 15.0 | 18.8 | 791650 | 3.81 | 11.8 | 15.6 |
| ycsb-b | rocksdb | 1214 MiB | 0 | 158422 | 17.4 | 23.2 | 26.6 | 791650 | 17.9 | 37.1 | 40.4 |
| ycsb-c | pigeonhole | 1824 MiB | 0 | 162648 | 8.26 | 14.5 | 17.9 | 837352 | 3.04 | 11.3 | 14.9 |
| ycsb-c | rocksdb | 1205 MiB | 0 | 162648 | 17.2 | 23.0 | 26.5 | 837352 | 9.41 | 21.6 | 24.6 |
| ycsb-d | pigeonhole | 1744 MiB | 0 | 230659 | 4.93 | 16.0 | 25.9 | 719316 | 2.05 | 9.15 | 13.5 |
| ycsb-d | rocksdb | 1271 MiB | 0 | 230659 | 11.6 | 24.1 | 28.5 | 719316 | 7.13 | 21.9 | 26.5 |
| ycsb-f | pigeonhole | 1780 MiB | 0 | 110217 | 8.96 | 16.0 | 20.5 | 389488 | 4.80 | 13.6 | 17.7 |
| ycsb-f | rocksdb | 1263 MiB | 0 | 110217 | 12.9 | 24.8 | 29.1 | 389488 | 19.8 | 41.7 | 53.8 |
| sparse-wide | pigeonhole | 2448 MiB | 0 | 122676 | 3.33 | 15.6 | 19.6 | 477384 | 3.23 | 393 | 510 |
| sparse-wide | rocksdb | 2118 MiB | 0 | 122676 | 7.29 | 16.5 | 19.5 | 477384 | 6.43 | 1016 | 1507 |
| time-series-ttl | pigeonhole | 267 MiB | 0 | 166812 | 1.24 | 3.55 | 6.30 | 32311 | 1.06 | 3.10 | 5.63 |
| time-series-ttl | rocksdb | 182 MiB | 0 | 166812 | 1.06 | 5.15 | 7.10 | 32311 | 0.90 | 3.89 | 5.54 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 11.0 | 10.4 | 16.4 | 30.1 |
| ycsb-a | pigeonhole | row read | 499915 | 6.17 | 5.28 | 15.6 | 23.0 |
| ycsb-a | rocksdb | put | 500085 | 3.44 | 3.23 | 6.17 | 8.57 |
| ycsb-a | rocksdb | row read | 499915 | 18.4 | 18.2 | 40.4 | 47.4 |
| ycsb-b | pigeonhole | put | 49928 | 13.3 | 11.8 | 38.1 | 44.5 |
| ycsb-b | pigeonhole | row read | 950072 | 5.05 | 4.09 | 13.5 | 16.5 |
| ycsb-b | rocksdb | put | 49928 | 4.91 | 4.67 | 8.45 | 10.9 |
| ycsb-b | rocksdb | row read | 950072 | 19.8 | 17.8 | 36.9 | 40.2 |
| ycsb-d | pigeonhole | put | 50025 | 19.5 | 14.5 | 49.7 | 436 |
| ycsb-d | pigeonhole | row read | 949975 | 3.43 | 2.33 | 13.3 | 18.8 |
| ycsb-d | rocksdb | put | 50025 | 7.75 | 7.52 | 12.0 | 15.0 |
| ycsb-d | rocksdb | row read | 949975 | 9.41 | 8.64 | 22.7 | 27.1 |
| ycsb-e | pigeonhole | put | 49830 | 39.1 | 41.0 | 60.7 | 78.3 |
| ycsb-e | pigeonhole | scan | 950170 | 62.0 | 60.7 | 136 | 155 |
| ycsb-e | rocksdb | put | 49830 | 9.77 | 9.66 | 14.4 | 17.7 |
| ycsb-e | rocksdb | scan | 950170 | 91.1 | 88.6 | 190 | 203 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 15.6 | 14.4 | 25.6 | 127 |
| ycsb-f | pigeonhole | row read | 499705 | 6.25 | 5.47 | 14.8 | 18.6 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 7.38 | 5.02 | 21.2 | 25.5 |
| ycsb-f | rocksdb | row read | 499705 | 18.7 | 18.2 | 40.7 | 52.2 |
| sparse-wide | pigeonhole | get | 399998 | 3.02 | 2.35 | 10.3 | 13.6 |
| sparse-wide | pigeonhole | put | 200020 | 16.5 | 14.1 | 44.5 | 55.8 |
| sparse-wide | pigeonhole | row read | 200062 | 56.2 | 12.8 | 473 | 520 |
| sparse-wide | pigeonhole | scan | 199920 | 85.5 | 42.2 | 549 | 602 |
| sparse-wide | rocksdb | get | 399998 | 4.60 | 4.89 | 10.8 | 12.8 |
| sparse-wide | rocksdb | put | 200020 | 5.20 | 4.83 | 10.2 | 12.6 |
| sparse-wide | rocksdb | row read | 200062 | 122 | 15.7 | 1327 | 1532 |
| sparse-wide | rocksdb | scan | 199920 | 153 | 48.6 | 1376 | 1589 |
| time-series-ttl | pigeonhole | get | 199123 | 1.39 | 1.21 | 3.49 | 6.21 |
| time-series-ttl | pigeonhole | put at | 400947 | 10.6 | 9.86 | 33.8 | 45.8 |
| time-series-ttl | pigeonhole | scan | 399930 | 4.35 | 4.09 | 7.74 | 10.8 |
| time-series-ttl | rocksdb | get | 199123 | 1.58 | 1.02 | 5.05 | 7.01 |
| time-series-ttl | rocksdb | put at | 400947 | 3.01 | 2.88 | 5.02 | 8.00 |
| time-series-ttl | rocksdb | scan | 399930 | 5.95 | 5.57 | 10.4 | 12.5 |
| adjacency | pigeonhole | put | 99878 | 18.6 | 13.6 | 43.5 | 53.8 |
| adjacency | pigeonhole | scan | 900122 | 41.6 | 14.2 | 317 | 350 |
| adjacency | rocksdb | put | 99878 | 4.39 | 4.08 | 7.74 | 9.79 |
| adjacency | rocksdb | scan | 900122 | 63.0 | 18.4 | 522 | 573 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 19562 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 64119 | 12.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4662 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2222 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 46199 | 9.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 14641 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 3435 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 9448 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 12136 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 39887 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 5798 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 6174 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 4094 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 24408 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 21254 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 23582 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 5694 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 9695 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 13309 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 42605 | 8.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 7911 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 15062 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 8720 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 21865 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 9028 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 26487 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 1273 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 2197 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 7667 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 11110 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 6287 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 9554 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 633 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 6914 | 13.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 466 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1299 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2321 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1919 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1064 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 2109 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 564 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1655 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 875 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 2157 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 3699 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1008 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 1111 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 947 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 678 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1272 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 1015 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 1276 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 3684 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 261 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 840 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1782 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 1867 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 475 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 807 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 769 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 1859 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 1307 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 787 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 2508 | 5.0% | 1 → 1 | 0 | 0 | 0 |
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
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1549 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 2077 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 606 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1985 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 124 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 324 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1079 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 3602 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1415 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 3509 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 539 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1969 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 851 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 416 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 2225 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 1477 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 3725 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1490 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 503 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 1376 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 1273 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 2543 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 3846 | 7.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 894 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 756 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 219 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 2544 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1801 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 1233 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 1787 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 1477 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 811 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2933 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1168 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1187 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 4812 | 9.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1220 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1758 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1814 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1951 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 823 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1812 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 3116 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 962 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 2202 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 2220 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 2400 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 1876 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 827 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 2524 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 278 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 1527 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 2079 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 679 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 567 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1132 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 349 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 1139 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 579 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1023 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 1045 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 344 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 1917 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 1567 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 11616 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 54813 | 11.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 26814 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 34562 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 7077 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 22580 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 8388 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 30186 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 22783 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 6416 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 40517 | 8.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 22062 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 7335 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 3002 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 986 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 8813 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 20732 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 3945 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 8532 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 1927 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 16163 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 6984 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 6608 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 5829 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 18915 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 19511 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 4648 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 14141 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 20813 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 9864 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 11389 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 22344 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 3108 | 1.6% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 20110 | 10.1% | 4 → 4 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 342 | 0.2% | 14 → 13 | 0 | 1 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 475 | 0.2% | 13 → 13 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 41 | 0.0% | 2 → 2 | 0 | 2 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1221 | 0.6% | 10 → 9 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1396 | 0.7% | 8 → 6 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 54 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 35 | 0.0% | 1 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2818 | 1.4% | 9 → 8 | 0 | 1 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 39 | 0.0% | 1 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 65 | 0.0% | 3 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 35 | 0.0% | 1 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 44 | 0.0% | 3 → 1 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 38 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 417 | 0.2% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 147 | 0.1% | 4 → 5 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 165 | 0.1% | 3 → 1 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 39 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 130 | 0.1% | 2 → 3 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 55 | 0.0% | 3 → 3 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 40 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 133 | 0.1% | 3 → 3 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 28 | 0.0% | 3 → 3 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 13 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 27827 | 13.9% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 13649 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 23707 | 11.9% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 25179 | 12.6% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 45421 | 22.7% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 33249 | 16.6% | 3 → 3 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 16177 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 17388 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 11069 | 2.8% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 678 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 22103 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 10810 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 4556 | 1.1% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 18414 | 4.6% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 5606 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 3771 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 9707 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 31542 | 7.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 23239 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 3083 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2534 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 41439 | 10.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1768 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 38922 | 9.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 5691 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 11790 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 5969 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 23478 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1912 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 35830 | 8.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 4097 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 14827 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1992 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 11100 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 11564 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 2852 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 7039 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 9372 | 9.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 9520 | 9.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 9354 | 9.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 29334 | 29.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 21882 | 21.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 17926 | 17.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 44 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 967 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 57 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 208 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 104 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 66 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 118 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 29 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 119 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 27 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 35 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 78 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 95 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 60 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 62 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 23 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 62 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 53 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 53 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 22 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 60 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 35 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 9 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 55 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 42 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 49164 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 128077 | 6.4% | 0 → 1 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 29949 | 1.5% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 69216 | 3.5% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 84562 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 102572 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 46970 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 81260 | 4.1% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 82487 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 72465 | 3.6% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 24431 | 1.2% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 71214 | 3.6% | 2 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 107070 | 5.4% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 76558 | 3.8% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 63268 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 47997 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 21061 | 1.1% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 50183 | 2.5% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 93187 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 52753 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 28917 | 1.4% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 29225 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 71226 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 74319 | 3.7% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 19009 | 1.0% | 1 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 38108 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 84381 | 4.2% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 110630 | 5.5% | 1 → 2 | 3 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 49505 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 41063 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 56970 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 42203 | 2.1% | 1 → 2 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 9589 | 4.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 44537 | 22.3% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 10453 | 5.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 4486 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 5063 | 2.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 1718 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 3444 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 11748 | 5.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 6360 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 2977 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 4024 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 5088 | 2.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 2994 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 2088 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 2041 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 2150 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 3090 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 12642 | 6.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 732 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 2416 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 4876 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 1710 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 1096 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 2162 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 5167 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 13639 | 6.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 3345 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 4893 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 3200 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 8830 | 4.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 11669 | 5.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 1773 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 6795 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 54951 | 27.5% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 3883 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 2929 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 3216 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 3946 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 7401 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 5140 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 4678 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 1980 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 4744 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 2151 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 7289 | 3.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 6048 | 3.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 4897 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 12681 | 6.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 920 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 9439 | 4.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 778 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 7041 | 3.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 6495 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 7002 | 3.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 3051 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 1886 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 5005 | 2.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 7408 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 2193 | 1.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 624 | 0.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 3463 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 3341 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 1168 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 7457 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 5223 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 117153 | 58.6% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 2317 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 629 | 0.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 1989 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 3460 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 3835 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 2716 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 4646 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 2021 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 2021 | 1.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 1030 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 1255 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 4624 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 721 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 1253 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 3499 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 1781 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 5269 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 1888 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 2375 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 4620 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 1208 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 2495 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 700 | 0.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 8530 | 4.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 989 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 2831 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 1049 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 2638 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 2626 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 2609 | 1.3% | 0 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 5 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.49 | 0.49 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 | 0.0 | 7 | 3 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 6 | 2 | 0 (0) | 0 | 0 | 8 | 10.1 | 0.07 | 0.05 | 0.05 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 | 0.0 | 6 | 3 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.50 | 0.50 | 0.01 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 1 | 1 | 0 (0) | 0 | 0 | 1 | 0.2 | 0.21 | 0.20 | 0.05 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 0 | 0.0 | 1 | 0 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.39 | 0.39 | 0.01 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 0.10 | 0.05 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 89 | 15.5 | 19 | 20 | 0 (0) | 0 | 0 | 20 | 9.7 | 0.65 | 0.65 | 0.02 | 1220 (53) | 0 |
| group-commit | pigeonhole | 32 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.07 | 0.00 | 193513 (44931) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 2 | 0.2 | 1.59 | 1.79 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 2 | 0.2 | 1.37 | 1.46 | 1.00 | 37475 (874) | 0 |
| group-commit | pigeonhole | 16 | 1 | 27.9 | 2 | 0 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.59 | 0.69 | 1.00 | 58373 (4062) | 0 |

**Durable group commit (D213):** 32 shards on 32 cores, the table split over every shard, 16 commits in flight per shard: 2.45M cells/s (76.7K per core), commit p50 242 µs, p99 1401 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 3% of the commits; 0 tablet changes while measuring; inside the engine p50 131 µs, p99 524 µs, p99.9 3146 µs; 0 WAL rollover blocks, 0 inline WAL syncs — fail.
