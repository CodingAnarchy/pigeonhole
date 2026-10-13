**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.99 [non-reference (D5)]
Build: release, rev a9ecfc773395. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1665086 | 1 | 166.2K | 4.83 | 14.6 | 27.9 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1009268 | 1 | 100.6K | 6.21 | 28.7 | 37.4 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3110198 | 1 | 285.9K | 1.02 | 13.2 | 15.9 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 90.1K | 9.28 | 22.3 | 25.3 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4141114 | 1 | 391.3K | 0.89 | 12.1 | 14.9 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1661371 | 1 | 160.0K | 5.50 | 11.0 | 13.8 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2610169 | 1 | 254.5K | 1.12 | 14.4 | 196 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1281479 | 1 | 122.0K | 7.87 | 15.4 | 18.0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 14.1K | 68.1 | 182 | 225 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.6K | 83.5 | 188 | 196 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1246905 | 1 | 122.5K | 6.56 | 17.5 | 510 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 85.8K | 10.4 | 30.8 | 39.7 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 27.3K | 10.9 | 432 | 578 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 18.3K | 6.85 | 938 | 1229 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2285395 | 1 | 217.2K | 4.35 | 9.98 | 15.9 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2395677 | 1 | 213.5K | 3.28 | 12.4 | 14.8 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 25.0K | 13.4 | 313 | 348 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 14.9K | 17.3 | 594 | 659 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4859651 | 4 | 469.1K | 6.69 | 13.2 | 512 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7078670 | 4 | 652.6K | 6.27 | 10.2 | 13.2 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 24000000 | 16 | 2.40M | 85.0 | 144 | 831 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 24000000 | 16 | 2.34M | 83.5 | 179 | 516 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.0K | 49.4 | 129 | 173 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 624222 | 4 | 60.1K | 59.4 | 140 | 184 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 1797399 | 16 | 179.4K | 75.8 | 178 | 1507 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.5K | 63.0 | 77.8 | 100 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 293034 | 4 | 28.4K | 142 | 170 | 203 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 1147353 | 16 | 101.9K | 172 | 221 | 293 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1664 MiB | 0 | 151971 | 9.21 | 18.0 | 25.6 | 680133 | 5.09 | 15.0 | 22.1 |
| ycsb-a | rocksdb | 1257 MiB | 0 | 110391 | 10.9 | 16.4 | 18.9 | 394044 | 18.4 | 31.6 | 41.5 |
| ycsb-b | pigeonhole | 1664 MiB | 0 | 277551 | 8.26 | 14.1 | 17.4 | 2677122 | 0.92 | 12.7 | 15.6 |
| ycsb-b | rocksdb | 1211 MiB | 0 | 158422 | 8.96 | 12.7 | 15.4 | 791650 | 9.73 | 22.5 | 25.6 |
| ycsb-c | pigeonhole | 1536 MiB | 0 | 312459 | 7.46 | 13.4 | 17.5 | 3828655 | 0.79 | 11.6 | 14.7 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 214123 | 8.19 | 11.8 | 14.4 | 1447248 | 4.42 | 10.9 | 13.7 |
| ycsb-d | pigeonhole | 1728 MiB | 0 | 458360 | 7.84 | 14.3 | 17.9 | 2021628 | 0.88 | 10.9 | 15.0 |
| ycsb-d | rocksdb | 1289 MiB | 0 | 277748 | 10.3 | 15.9 | 18.7 | 939609 | 5.70 | 15.2 | 17.8 |
| ycsb-f | pigeonhole | 1664 MiB | 0 | 127496 | 8.64 | 15.5 | 19.5 | 495733 | 4.45 | 14.0 | 18.4 |
| ycsb-f | rocksdb | 1254 MiB | 0 | 110217 | 10.8 | 16.3 | 18.9 | 389488 | 19.1 | 34.3 | 42.2 |
| sparse-wide | pigeonhole | 2432 MiB | 0 | 122676 | 3.55 | 16.2 | 19.8 | 477384 | 3.42 | 418 | 532 |
| sparse-wide | rocksdb | 2123 MiB | 0 | 122676 | 6.78 | 15.8 | 18.4 | 477384 | 5.82 | 922 | 1196 |
| time-series-ttl | pigeonhole | 512 MiB | 0 | 374221 | 1.49 | 4.93 | 6.69 | 81988 | 1.29 | 4.80 | 6.56 |
| time-series-ttl | rocksdb | 254 MiB | 0 | 391908 | 1.14 | 5.09 | 7.04 | 86300 | 1.03 | 4.38 | 6.21 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 832982 | 6.13 | 4.67 | 10.9 | 498 |
| ycsb-a | pigeonhole | row read | 832104 | 5.82 | 5.92 | 15.9 | 23.2 |
| ycsb-a | rocksdb | put | 504833 | 3.40 | 3.21 | 5.95 | 8.38 |
| ycsb-a | rocksdb | row read | 504435 | 16.4 | 14.2 | 30.8 | 40.4 |
| ycsb-b | pigeonhole | put | 155525 | 9.61 | 9.66 | 14.2 | 17.7 |
| ycsb-b | pigeonhole | row read | 2954673 | 3.12 | 0.98 | 13.1 | 15.7 |
| ycsb-b | rocksdb | put | 49928 | 4.00 | 3.71 | 7.23 | 9.34 |
| ycsb-b | rocksdb | row read | 950072 | 11.4 | 9.41 | 22.3 | 25.3 |
| ycsb-d | pigeonhole | put | 130181 | 23.4 | 12.8 | 436 | 524 |
| ycsb-d | pigeonhole | row read | 2479988 | 2.85 | 1.06 | 12.7 | 15.8 |
| ycsb-d | rocksdb | put | 64122 | 7.72 | 7.52 | 11.7 | 14.2 |
| ycsb-d | rocksdb | row read | 1217357 | 8.15 | 8.00 | 15.4 | 18.0 |
| ycsb-e | pigeonhole | put | 49830 | 32.8 | 36.9 | 46.3 | 51.2 |
| ycsb-e | pigeonhole | scan | 950170 | 73.0 | 70.1 | 184 | 226 |
| ycsb-e | rocksdb | put | 49830 | 9.79 | 9.73 | 14.1 | 16.6 |
| ycsb-e | rocksdb | scan | 950170 | 90.1 | 88.1 | 188 | 196 |
| ycsb-f | pigeonhole | read-modify-write | 623676 | 11.0 | 7.68 | 19.6 | 518 |
| ycsb-f | pigeonhole | row read | 623229 | 5.25 | 4.96 | 14.5 | 18.8 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.52 | 4.80 | 14.5 | 17.8 |
| ycsb-f | rocksdb | row read | 499705 | 16.7 | 14.3 | 33.3 | 41.7 |
| sparse-wide | pigeonhole | get | 399998 | 3.37 | 2.70 | 11.4 | 14.8 |
| sparse-wide | pigeonhole | put | 200020 | 15.4 | 12.4 | 41.0 | 48.1 |
| sparse-wide | pigeonhole | row read | 200062 | 61.1 | 14.5 | 489 | 545 |
| sparse-wide | pigeonhole | scan | 199920 | 99.5 | 68.1 | 541 | 614 |
| sparse-wide | rocksdb | get | 399998 | 4.25 | 4.54 | 9.92 | 12.0 |
| sparse-wide | rocksdb | put | 200020 | 5.23 | 4.93 | 10.1 | 12.4 |
| sparse-wide | rocksdb | row read | 200062 | 114 | 15.4 | 1073 | 1237 |
| sparse-wide | rocksdb | scan | 199920 | 145 | 48.9 | 1130 | 1278 |
| time-series-ttl | pigeonhole | get | 456209 | 1.79 | 1.46 | 4.93 | 6.66 |
| time-series-ttl | pigeonhole | put at | 914885 | 5.82 | 4.64 | 10.4 | 16.0 |
| time-series-ttl | pigeonhole | scan | 914301 | 4.70 | 4.38 | 10.1 | 17.9 |
| time-series-ttl | rocksdb | get | 478208 | 1.66 | 1.12 | 4.99 | 6.94 |
| time-series-ttl | rocksdb | put at | 959155 | 3.11 | 2.98 | 5.21 | 8.00 |
| time-series-ttl | rocksdb | scan | 958314 | 7.67 | 7.07 | 13.2 | 16.4 |
| adjacency | pigeonhole | put | 99878 | 15.6 | 11.1 | 43.0 | 463 |
| adjacency | pigeonhole | scan | 900122 | 42.7 | 14.6 | 315 | 346 |
| adjacency | rocksdb | put | 99878 | 4.58 | 4.42 | 7.78 | 9.92 |
| adjacency | rocksdb | scan | 900122 | 74.1 | 21.5 | 602 | 664 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 622335 | 74.7% | 11 → 16 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 210647 | 25.3% | 5 → 0 | 0 | 0 | 5 |
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
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 66947 | 43.0% | 11 → 11 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 88578 | 57.0% | 5 → 5 | 0 | 0 | 0 |
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
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 12 → 12 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 7 → 7 | 0 | 0 | 0 |
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
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 60128 | 46.2% | 8 → 8 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 70053 | 53.8% | 12 → 12 | 0 | 0 | 0 |
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
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 25955 | 52.1% | 11 → 11 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 23875 | 47.9% | 5 → 5 | 0 | 0 | 0 |
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
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 474973 | 76.2% | 11 → 16 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 148703 | 23.8% | 5 → 0 | 0 | 0 | 5 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 60480 | 30.2% | 10 → 11 | 0 | 1 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 39836 | 19.9% | 7 → 7 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 15758 | 7.9% | 16 → 14 | 0 | 1 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 83764 | 41.9% | 16 → 15 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 16 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 53 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 7 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 11 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 65 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 16 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 14 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 527342 | 57.6% | 10 → 11 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 387543 | 42.4% | 10 → 9 | 0 | 0 | 1 |
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
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 210 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 87406 | 87.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 166 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 361 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 224 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 366 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1450 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 640 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 330 | 0.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 189 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 423 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1784 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 425 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 241 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 175 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 5488 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2462517 | 50.7% | 7 → 7 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 2397134 | 49.3% | 12 → 12 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 9029 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 8498 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 9782 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 17418 | 8.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 4151 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 3946 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 23522 | 11.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 15962 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 20267 | 10.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 7219 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 6618 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 2182 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 31718 | 15.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 8413 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 13412 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 17863 | 8.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 329441 | 52.8% | 1 → 11 | 1 | 0 | 4 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 176740 | 28.3% | 1 → 6 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 10923 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 13224 | 2.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 4842 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 6268 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 5033 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 5920 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 7813 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 2447 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 12296 | 2.0% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 9817 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 14023 | 2.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 8378 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 10109 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 6948 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 373371 | 20.8% | 1 → 4 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 592304 | 33.0% | 0 → 4 | 0 | 0 | 7 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 341489 | 19.0% | 1 → 4 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 328061 | 18.3% | 1 → 6 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 120714 | 6.7% | 2 → 0 | 0 | 0 | 3 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 3516 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 2254 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 2590 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 8856 | 0.5% | 2 → 0 | 0 | 0 | 2 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 1597 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 2564 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 4387 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 7968 | 0.4% | 2 → 0 | 0 | 0 | 2 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 2464 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 3104 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 2160 | 0.1% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 2 | 73.6 | 12 | 2 | 1 (5) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 6 | 234.3 | 14 | 10 | 1 (2) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.05 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 2 | 51.8 | 10 | 7 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.06 | 0.07 | 0.00 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.20 | 0.20 | 0.05 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 3 | 130.4 | 10 | 8 | 2 (9) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 1 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.02 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 8 | 205.2 | 48 | 28 | 9 (47) | 0 | 0 | 0 | 0.0 | 0.00 | 0.03 | 0.01 | 195 (0) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 971 | 240 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.11 | 0.00 | 174764 (28133) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 977 | 240 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.11 | 0.00 | 2106570 (443819) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 3 | 3 | 0 (0) | 0 | 0 | 1 | 0.1 | 1.99 | 1.99 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 4 | 17.8 | 35 | 13 | 1 (1) | 0 | 0 | 6 | 0.4 | 0.48 | 0.87 | 1.00 | 358 (4) | 0 |
| group-commit | pigeonhole | 16 | 10 | 67.5 | 89 | 28 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.13 | 0.51 | 1.00 | 4909 (305) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 9.60M cells/s (599.8K per core), commit p50 85.0 µs, p99 144 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 65.5 µs, p99 131 µs, p99.9 786 µs; 0 WAL rollover blocks, 0 inline WAL syncs — pass.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 9.37M cells/s, commit p50 83.5 µs, p99 179 µs.
