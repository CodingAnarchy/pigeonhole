**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.10 [non-reference (D5)]
Build: release, rev f333d4879d6d. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 113.2K | 9.47 | 16.4 | 37.6 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 91.0K | 6.43 | 38.1 | 44.3 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 171.7K | 4.48 | 17.2 | 39.9 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 52.4K | 17.5 | 36.6 | 40.2 |
| ycsb-c | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 216.5K | 3.49 | 13.1 | 15.6 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 79.2K | 13.5 | 22.0 | 25.1 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 249.1K | 2.33 | 16.5 | 39.7 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 106.5K | 8.51 | 22.5 | 26.4 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 16.2K | 58.9 | 136 | 153 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.4K | 84.5 | 191 | 203 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 89.7K | 11.1 | 24.1 | 47.9 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 76.7K | 10.8 | 39.2 | 46.8 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 30.3K | 10.9 | 418 | 582 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 17.3K | 7.17 | 1016 | 1540 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 163.0K | 4.86 | 13.5 | 17.3 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 253.6K | 3.15 | 9.41 | 11.9 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 25.3K | 13.8 | 317 | 352 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 17.3K | 14.7 | 518 | 590 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2000000 | 4 | 318.7K | 10.9 | 23.3 | 204 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2000000 | 4 | 549.3K | 6.27 | 11.5 | 14.5 |
| group-commit | pigeonhole | shards=32 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 | 1000 | 200000 | 32 | 653.2K | 212 | 860 | 3588 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.9K | 49.2 | 119 | 159 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 4 | 51.6K | 66.6 | 136 | 175 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 16 | 113.8K | 120 | 225 | 1040 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.4K | 63.2 | 78.3 | 101 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 4 | 27.8K | 143 | 171 | 206 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 16 | 95.7K | 174 | 244 | 279 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1840 MiB | 0 | 109752 | 9.15 | 18.0 | 25.6 | 390163 | 4.89 | 14.1 | 19.3 |
| ycsb-a | rocksdb | 1263 MiB | 0 | 109752 | 13.0 | 25.6 | 30.1 | 390163 | 19.7 | 40.4 | 48.4 |
| ycsb-b | pigeonhole | 1680 MiB | 0 | 158422 | 8.45 | 15.1 | 19.5 | 791650 | 4.01 | 12.2 | 15.9 |
| ycsb-b | rocksdb | 1214 MiB | 0 | 158422 | 17.4 | 23.3 | 26.8 | 791650 | 18.0 | 36.9 | 40.4 |
| ycsb-c | pigeonhole | 1792 MiB | 0 | 162648 | 8.32 | 14.4 | 17.9 | 837352 | 3.04 | 11.5 | 15.0 |
| ycsb-c | rocksdb | 1205 MiB | 0 | 162648 | 17.2 | 23.2 | 26.8 | 837352 | 9.47 | 21.6 | 24.6 |
| ycsb-d | pigeonhole | 1936 MiB | 0 | 230659 | 4.93 | 15.6 | 22.9 | 719316 | 2.01 | 8.83 | 12.7 |
| ycsb-d | rocksdb | 1271 MiB | 0 | 230659 | 11.6 | 23.8 | 27.8 | 719316 | 7.13 | 21.8 | 25.7 |
| ycsb-f | pigeonhole | 1744 MiB | 0 | 110217 | 9.09 | 16.6 | 21.8 | 389488 | 5.02 | 14.4 | 18.6 |
| ycsb-f | rocksdb | 1263 MiB | 0 | 110217 | 12.7 | 25.1 | 29.2 | 389488 | 20.0 | 41.5 | 52.0 |
| sparse-wide | pigeonhole | 2416 MiB | 0 | 122676 | 3.34 | 15.6 | 19.3 | 477384 | 3.20 | 395 | 510 |
| sparse-wide | rocksdb | 2118 MiB | 0 | 122676 | 7.26 | 16.5 | 19.3 | 477384 | 6.37 | 1020 | 1516 |
| time-series-ttl | pigeonhole | 256 MiB | 0 | 166812 | 1.22 | 3.52 | 5.92 | 32311 | 1.04 | 3.15 | 5.31 |
| time-series-ttl | rocksdb | 182 MiB | 0 | 166812 | 1.06 | 5.25 | 7.10 | 32311 | 0.90 | 3.95 | 5.73 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 11.1 | 10.5 | 17.3 | 43.8 |
| ycsb-a | pigeonhole | row read | 499915 | 6.45 | 5.47 | 15.7 | 22.5 |
| ycsb-a | rocksdb | put | 500085 | 3.42 | 3.23 | 6.11 | 8.57 |
| ycsb-a | rocksdb | row read | 499915 | 18.5 | 18.0 | 39.9 | 46.8 |
| ycsb-b | pigeonhole | put | 49928 | 15.6 | 12.4 | 42.0 | 47.9 |
| ycsb-b | pigeonhole | row read | 950072 | 5.26 | 4.32 | 13.6 | 16.9 |
| ycsb-b | rocksdb | put | 49928 | 4.99 | 4.80 | 8.64 | 11.1 |
| ycsb-b | rocksdb | row read | 950072 | 19.8 | 17.8 | 36.6 | 40.2 |
| ycsb-d | pigeonhole | put | 50025 | 15.7 | 13.8 | 42.0 | 88.1 |
| ycsb-d | pigeonhole | row read | 949975 | 3.35 | 2.25 | 13.1 | 17.3 |
| ycsb-d | rocksdb | put | 50025 | 7.79 | 7.55 | 12.1 | 14.9 |
| ycsb-d | rocksdb | row read | 949975 | 9.43 | 8.64 | 22.5 | 26.4 |
| ycsb-e | pigeonhole | put | 49830 | 39.6 | 41.5 | 61.4 | 78.8 |
| ycsb-e | pigeonhole | scan | 950170 | 62.8 | 61.4 | 137 | 153 |
| ycsb-e | rocksdb | put | 49830 | 9.91 | 9.79 | 14.5 | 18.0 |
| ycsb-e | rocksdb | scan | 950170 | 91.4 | 88.6 | 191 | 204 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 15.8 | 14.6 | 26.4 | 94.7 |
| ycsb-f | pigeonhole | row read | 499705 | 6.40 | 5.60 | 15.4 | 19.6 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 7.39 | 4.99 | 21.5 | 25.9 |
| ycsb-f | rocksdb | row read | 499705 | 18.6 | 18.2 | 41.0 | 50.4 |
| sparse-wide | pigeonhole | get | 399998 | 3.00 | 2.33 | 10.4 | 13.6 |
| sparse-wide | pigeonhole | put | 200020 | 16.2 | 13.5 | 43.5 | 53.0 |
| sparse-wide | pigeonhole | row read | 200062 | 56.6 | 12.9 | 471 | 518 |
| sparse-wide | pigeonhole | scan | 199920 | 85.7 | 42.0 | 549 | 606 |
| sparse-wide | rocksdb | get | 399998 | 4.57 | 4.89 | 10.7 | 12.7 |
| sparse-wide | rocksdb | put | 200020 | 5.18 | 4.80 | 10.2 | 12.3 |
| sparse-wide | rocksdb | row read | 200062 | 122 | 15.6 | 1335 | 1548 |
| sparse-wide | rocksdb | scan | 199920 | 153 | 48.4 | 1376 | 1606 |
| time-series-ttl | pigeonhole | get | 199123 | 1.38 | 1.18 | 3.47 | 5.82 |
| time-series-ttl | pigeonhole | put at | 400947 | 10.1 | 9.79 | 14.7 | 23.0 |
| time-series-ttl | pigeonhole | scan | 399930 | 4.47 | 4.25 | 7.78 | 10.3 |
| time-series-ttl | rocksdb | get | 199123 | 1.58 | 1.02 | 5.15 | 7.01 |
| time-series-ttl | rocksdb | put at | 400947 | 3.02 | 2.90 | 5.02 | 8.00 |
| time-series-ttl | rocksdb | scan | 399930 | 5.94 | 5.57 | 10.4 | 12.6 |
| adjacency | pigeonhole | put | 99878 | 18.3 | 13.2 | 43.8 | 53.8 |
| adjacency | pigeonhole | scan | 900122 | 41.8 | 14.1 | 322 | 352 |
| adjacency | rocksdb | put | 99878 | 4.38 | 4.06 | 7.78 | 9.92 |
| adjacency | rocksdb | scan | 900122 | 63.7 | 18.6 | 524 | 590 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 41391 | 8.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 74545 | 14.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 33599 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 21435 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 16252 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 8273 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 7511 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 6578 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 9018 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 7342 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 13437 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 30104 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 22828 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 11958 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 9935 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 10441 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 6157 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 2604 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 19300 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 6772 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 1953 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 2067 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 1415 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 10822 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 4544 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 5370 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 13394 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 13524 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 2083 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 42637 | 8.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 17981 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 24815 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 262 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 6466 | 13.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 955 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1106 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1637 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 3342 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 660 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1157 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 609 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 3674 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 929 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 809 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 2071 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 3980 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 3189 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 1161 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 737 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 520 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 317 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 500 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 897 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 501 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 571 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 469 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 408 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 1603 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 1589 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1788 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 643 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 246 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 4576 | 9.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 2556 | 5.1% | 1 → 1 | 0 | 0 | 0 |
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
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 762 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1090 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1925 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 286 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 3145 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 808 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2117 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 2577 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1635 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1079 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 2627 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 2636 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 3082 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1395 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 1629 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 1683 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 421 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1248 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 3235 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 2293 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 1090 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 445 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 914 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 1023 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 592 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 844 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 2902 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1771 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 236 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 2765 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 988 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 782 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1306 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1759 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1912 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1834 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2238 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 720 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1143 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1579 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1481 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1382 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 1890 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1250 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 376 | 0.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1289 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 4593 | 9.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2001 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 2170 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 1053 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 1417 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 509 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 1830 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 1432 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 1947 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 2378 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 487 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 2754 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 1378 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 1553 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 752 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 1628 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 723 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 1066 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 7852 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 81961 | 16.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 15103 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 11638 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 13979 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 17556 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 3432 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 8003 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 6934 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 10398 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 12141 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 9581 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 4882 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 23467 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 12672 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 30135 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 8751 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 4914 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 12215 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 6878 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 11951 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 14239 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 8587 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 35572 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 5829 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 17603 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 8829 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 5034 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 26022 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 30979 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 17154 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 16004 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 5529 | 2.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 19603 | 9.8% | 3 → 3 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1693 | 0.8% | 15 → 15 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 634 | 0.3% | 4 → 5 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1490 | 0.7% | 7 → 5 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 14 | 0.0% | 1 → 2 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1528 | 0.8% | 9 → 8 | 0 | 1 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 9 | 0.0% | 2 → 1 | 0 | 1 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 14 | 0.0% | 2 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 1 → 0 | 0 | 1 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 841 | 0.4% | 6 → 4 | 0 | 1 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 86 | 0.0% | 3 → 4 | 0 | 1 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 16 | 0.0% | 2 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 1 → 0 | 0 | 2 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 223 | 0.1% | 3 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 9 | 0.0% | 2 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 275 | 0.1% | 4 → 5 | 0 | 2 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 116 | 0.1% | 3 → 2 | 0 | 2 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 0 | 0.0% | 2 → 0 | 0 | 1 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 8 | 0.0% | 2 → 1 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 2 | 0.0% | 1 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 114 | 0.1% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 16 | 0.0% | 1 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 17109 | 8.6% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 19817 | 9.9% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 23719 | 11.9% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 14891 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 54591 | 27.3% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 37673 | 18.8% | 3 → 3 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 21522 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 8344 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 9259 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 8459 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 12888 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 9479 | 2.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2827 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 27827 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 13079 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 15427 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 2289 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 16744 | 4.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 3671 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 5132 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 42414 | 10.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 9067 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 27700 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 8036 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 9987 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 12445 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 20401 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 11473 | 2.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 21838 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 6779 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 9390 | 2.3% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 13018 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 2686 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 11279 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 8372 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 13655 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 8532 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 6928 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 34207 | 34.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 25018 | 25.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 16674 | 16.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 19508 | 19.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1752 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 109 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 32 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 48 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 75 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 19 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 924 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 29 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 54 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 117 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 17 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 120 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 206 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 53 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 18 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 147 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 59 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 147 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 47 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 54 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 27 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 18 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 41 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 26 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 177 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 117 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 31 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 63248 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 123287 | 6.2% | 0 → 1 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 107461 | 5.4% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 47744 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 69651 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 55191 | 2.8% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 73142 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 100667 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 40928 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 98992 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 37122 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 38639 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 67075 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 136156 | 6.8% | 2 → 1 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 37676 | 1.9% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 64063 | 3.2% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 16 | 29688 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 17 | 29943 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 18 | 26514 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 19 | 44534 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 20 | 82018 | 4.1% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 21 | 71442 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 22 | 81685 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 23 | 57715 | 2.9% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 24 | 32013 | 1.6% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 25 | 52344 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 26 | 111247 | 5.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27 | 41009 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 28 | 27341 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 29 | 22986 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 30 | 69432 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 31 | 59047 | 3.0% | 1 → 2 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 4739 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 43325 | 21.7% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 11214 | 5.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 7119 | 3.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 1282 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 3723 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 7525 | 3.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 8934 | 4.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 7186 | 3.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 7647 | 3.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 1620 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 10894 | 5.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 3315 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 917 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 3963 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 9248 | 4.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 6545 | 3.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 5497 | 2.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 1434 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 1769 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 4020 | 2.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 3638 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 2426 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 5131 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 6808 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 3617 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 2584 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 7394 | 3.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 6153 | 3.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 2703 | 1.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 1781 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 5849 | 2.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 9641 | 4.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 56225 | 28.1% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 9942 | 5.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 3804 | 1.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 7544 | 3.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 2512 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 4260 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 1713 | 0.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 2672 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 6427 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 9614 | 4.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 3348 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 11437 | 5.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 1330 | 0.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 1562 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 3228 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 13604 | 6.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 5375 | 2.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 6305 | 3.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 5849 | 2.9% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 3372 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 1211 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 6089 | 3.0% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 3219 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 1039 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 2923 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 4570 | 2.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 4383 | 2.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 3106 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 2330 | 1.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 925 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 441 | 0.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 773 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 121166 | 60.6% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 1637 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 3355 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 2566 | 1.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 1577 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 4161 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 3550 | 1.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 957 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 1536 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 6894 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 6797 | 3.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 3027 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 2978 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 4217 | 2.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 4895 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 16 | 266 | 0.1% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 17 | 5233 | 2.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 18 | 405 | 0.2% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 19 | 801 | 0.4% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 20 | 3300 | 1.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 21 | 1262 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 22 | 3422 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 23 | 3081 | 1.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 24 | 638 | 0.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 25 | 688 | 0.3% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 26 | 1627 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 27 | 3479 | 1.7% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 28 | 1591 | 0.8% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 29 | 1199 | 0.6% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 30 | 942 | 0.5% | 0 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 31 | 1980 | 1.0% | 0 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 7 | 2 | 0 (0) | 0 | 0 | 2 | 8.4 | 0.49 | 0.49 | 0.01 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 2 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 | 0.0 | 6 | 0 | 0 (0) | 0 | 0 | 2 | 0.3 | 0.05 | 0.05 | 0.01 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 7 | 3 | 0 (0) | 0 | 0 | 4 | 8.6 | 0.07 | 0.05 | 0.05 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 | 0.0 | 8 | 3 | 0 (0) | 0 | 0 | 7 | 9.8 | 0.50 | 0.49 | 0.01 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.21 | 0.20 | 0.06 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.40 | 0.40 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 0.10 | 0.04 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 113 | 18.9 | 15 | 13 | 0 (0) | 0 | 0 | 16 | 3.1 | 0.66 | 0.66 | 0.03 | 1489 (48) | 0 |
| group-commit | pigeonhole | 32 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.07 | 0.00 | 167311 (37308) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 2 | 0.2 | 1.63 | 1.81 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 2 | 0.2 | 1.36 | 1.45 | 1.00 | 37626 (897) | 0 |
| group-commit | pigeonhole | 16 | 1 | 31.7 | 3 | 0 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.53 | 0.64 | 1.00 | 53780 (4062) | 0 |

**Durable group commit (D213):** 32 shards on 32 cores, the table split over every shard, 16 commits in flight per shard: 2.61M cells/s (81.7K per core), commit p50 212 µs, p99 860 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 3% of the commits; 0 tablet changes while measuring; inside the engine p50 131 µs, p99 524 µs, p99.9 2097 µs; 0 WAL rollover blocks, 0 inline WAL syncs — fail.
