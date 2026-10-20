**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 13.60 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1846573 | 1 | 178.3K | 4.83 | 14.7 | 21.1 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1006496 | 1 | 100.4K | 6.27 | 28.9 | 36.6 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3283850 | 1 | 300.2K | 1.05 | 12.9 | 15.6 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 90.9K | 9.21 | 21.8 | 25.1 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 4044876 | 1 | 384.9K | 0.92 | 12.2 | 14.9 |
| ycsb-c | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1659753 | 1 | 160.3K | 5.47 | 11.0 | 13.9 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3004864 | 1 | 287.0K | 1.14 | 15.2 | 19.5 |
| ycsb-d | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1274398 | 1 | 121.3K | 7.84 | 15.4 | 17.8 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 15.3K | 64.3 | 153 | 211 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 11.6K | 83.5 | 188 | 196 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 92.6K | 10.6 | 26.6 | 32.4 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 86.1K | 10.2 | 30.0 | 37.6 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 25.0K | 11.5 | 463 | 631 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 18.3K | 6.82 | 934 | 1229 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2381535 | 1 | 232.3K | 4.32 | 8.57 | 13.6 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2387080 | 1 | 212.4K | 3.25 | 12.2 | 14.8 |
| adjacency | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2000000 | 1000000 | 1 | 26.0K | 12.0 | 309 | 332 |
| adjacency | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 2000000 | 1000000 | 1 | 12.0K | 19.2 | 795 | 877 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 7898572 | 4 | 715.9K | 5.09 | 9.79 | 14.2 |
| skewed-multi-shard | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6727711 | 4 | 633.4K | 6.40 | 10.8 | 13.9 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1000 | 28000000 | 16 | 2.66M | 71.7 | 137 | 971 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 unpinned | 1000 | 26000000 | 16 | 2.43M | 78.3 | 170 | 324 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 200000 | 1 | 17.4K | 49.4 | 116 | 148 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 756111 | 4 | 68.5K | 45.8 | 140 | 168 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1000 | 2268096 | 16 | 226.7K | 56.8 | 162 | 465 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 200000 | 1 | 15.5K | 63.0 | 77.8 | 99.8 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 291666 | 4 | 28.3K | 143 | 169 | 205 |
| group-commit | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync | 1000 | 1088215 | 16 | 99.5K | 168 | 223 | 287 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 7016520 | 32 | 583.8K | 41.7 | 134 | 371 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6838137 | 32 | 591.7K | 41.0 | 136 | 389 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1000000 | 7031412 | 32 | 589.2K | 40.7 | 133 | 442 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 6400548 | 32 | 567.5K | 50.9 | 104 | 315 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1792 MiB | 0 | 161565 | 9.41 | 18.2 | 36.4 | 761197 | 5.34 | 15.3 | 22.5 |
| ycsb-a | rocksdb | 1257 MiB | 0 | 110179 | 10.8 | 16.5 | 19.1 | 392896 | 18.7 | 31.5 | 41.2 |
| ycsb-b | pigeonhole | 2240 MiB | 0 | 283472 | 8.32 | 14.0 | 16.5 | 2836118 | 0.91 | 12.4 | 15.6 |
| ycsb-b | rocksdb | 1211 MiB | 0 | 158422 | 8.89 | 12.7 | 15.3 | 791650 | 9.66 | 21.9 | 25.2 |
| ycsb-c | pigeonhole | 2176 MiB | 0 | 310040 | 7.62 | 13.4 | 17.3 | 3734836 | 0.85 | 11.7 | 14.7 |
| ycsb-c | rocksdb | 1201 MiB | 0 | 214015 | 8.19 | 11.9 | 14.3 | 1445738 | 4.42 | 10.9 | 13.7 |
| ycsb-d | pigeonhole | 2240 MiB | 0 | 503182 | 8.06 | 14.3 | 18.3 | 2351635 | 0.92 | 11.1 | 15.2 |
| ycsb-d | rocksdb | 1288 MiB | 0 | 276581 | 10.4 | 15.9 | 18.4 | 934035 | 5.86 | 15.2 | 17.5 |
| ycsb-f | pigeonhole | 2176 MiB | 0 | 110217 | 9.02 | 16.5 | 22.4 | 389488 | 5.12 | 14.6 | 19.5 |
| ycsb-f | rocksdb | 1254 MiB | 0 | 110217 | 10.7 | 16.2 | 19.1 | 389488 | 18.9 | 32.6 | 40.7 |
| sparse-wide | pigeonhole | 2624 MiB | 0 | 122676 | 3.95 | 17.5 | 21.6 | 477384 | 3.90 | 438 | 553 |
| sparse-wide | rocksdb | 2123 MiB | 0 | 122676 | 6.78 | 15.6 | 18.2 | 477384 | 5.86 | 922 | 1196 |
| time-series-ttl | pigeonhole | 512 MiB | 0 | 389634 | 1.53 | 4.86 | 6.66 | 85709 | 1.37 | 4.74 | 6.46 |
| time-series-ttl | rocksdb | 254 MiB | 0 | 390521 | 1.16 | 5.02 | 7.07 | 85935 | 1.05 | 4.32 | 6.11 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 923811 | 5.00 | 4.67 | 7.71 | 11.4 |
| ycsb-a | pigeonhole | row read | 922762 | 6.12 | 6.30 | 16.0 | 24.8 |
| ycsb-a | rocksdb | put | 503421 | 3.34 | 3.15 | 5.89 | 8.19 |
| ycsb-a | rocksdb | row read | 503075 | 16.4 | 14.3 | 30.7 | 39.9 |
| ycsb-b | pigeonhole | put | 164260 | 5.46 | 4.89 | 10.9 | 14.7 |
| ycsb-b | pigeonhole | row read | 3119590 | 3.16 | 1.01 | 12.9 | 15.6 |
| ycsb-b | rocksdb | put | 49928 | 3.83 | 3.52 | 7.04 | 9.21 |
| ycsb-b | rocksdb | row read | 950072 | 11.3 | 9.34 | 21.8 | 25.1 |
| ycsb-d | pigeonhole | put | 150047 | 13.4 | 13.6 | 20.2 | 27.6 |
| ycsb-d | pigeonhole | row read | 2854817 | 2.90 | 1.09 | 12.7 | 15.9 |
| ycsb-d | rocksdb | put | 63782 | 7.51 | 7.33 | 11.6 | 14.0 |
| ycsb-d | rocksdb | row read | 1210616 | 8.21 | 8.03 | 15.4 | 17.8 |
| ycsb-e | pigeonhole | put | 49830 | 34.7 | 36.1 | 42.2 | 46.8 |
| ycsb-e | pigeonhole | scan | 950170 | 67.0 | 66.6 | 153 | 212 |
| ycsb-e | rocksdb | put | 49830 | 9.48 | 9.34 | 13.9 | 16.5 |
| ycsb-e | rocksdb | scan | 950170 | 90.0 | 87.6 | 188 | 196 |
| ycsb-f | pigeonhole | read-modify-write | 500295 | 15.7 | 15.0 | 28.3 | 35.1 |
| ycsb-f | pigeonhole | row read | 499705 | 5.72 | 5.82 | 15.2 | 20.0 |
| ycsb-f | rocksdb | read-modify-write | 500295 | 6.51 | 4.80 | 14.4 | 17.5 |
| ycsb-f | rocksdb | row read | 499705 | 16.6 | 14.2 | 31.9 | 39.9 |
| sparse-wide | pigeonhole | get | 399998 | 3.78 | 3.07 | 12.2 | 15.9 |
| sparse-wide | pigeonhole | put | 200020 | 17.5 | 12.9 | 40.2 | 67.6 |
| sparse-wide | pigeonhole | row read | 200062 | 66.5 | 15.6 | 514 | 561 |
| sparse-wide | pigeonhole | scan | 199920 | 108 | 70.7 | 573 | 659 |
| sparse-wide | rocksdb | get | 399998 | 4.26 | 4.61 | 9.60 | 11.8 |
| sparse-wide | rocksdb | put | 200020 | 5.16 | 4.86 | 9.98 | 12.3 |
| sparse-wide | rocksdb | row read | 200062 | 114 | 15.3 | 1073 | 1229 |
| sparse-wide | rocksdb | scan | 199920 | 146 | 48.6 | 1130 | 1286 |
| time-series-ttl | pigeonhole | get | 475343 | 1.85 | 1.50 | 4.83 | 6.66 |
| time-series-ttl | pigeonhole | put at | 953532 | 4.89 | 4.54 | 7.49 | 10.3 |
| time-series-ttl | pigeonhole | scan | 952660 | 4.81 | 4.51 | 10.0 | 16.9 |
| time-series-ttl | rocksdb | get | 476456 | 1.67 | 1.13 | 4.96 | 6.91 |
| time-series-ttl | rocksdb | put at | 955745 | 3.10 | 2.98 | 5.15 | 8.03 |
| time-series-ttl | rocksdb | scan | 954879 | 7.67 | 7.17 | 13.1 | 16.3 |
| adjacency | pigeonhole | put | 99878 | 9.48 | 9.86 | 14.6 | 19.1 |
| adjacency | pigeonhole | scan | 900122 | 41.7 | 13.9 | 311 | 332 |
| adjacency | rocksdb | put | 99878 | 4.54 | 4.83 | 7.74 | 9.92 |
| adjacency | rocksdb | scan | 900122 | 92.3 | 23.9 | 803 | 881 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3509271 | 57.4 | 44.0 | 137 | 406 |
| cross-shard-contention | pigeonhole | put | 3507249 | 51.2 | 39.2 | 130 | 346 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3420206 | 57.0 | 43.0 | 139 | 426 |
| cross-shard-contention | pigeonhole | put | 3417931 | 50.0 | 38.4 | 132 | 354 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3516623 | 57.2 | 42.8 | 136 | 477 |
| cross-shard-contention | pigeonhole | put | 3514789 | 50.5 | 38.1 | 130 | 393 |
| cross-shard-contention | rocksdb | cross-shard commit | 3201711 | 57.8 | 52.7 | 108 | 319 |
| cross-shard-contention | rocksdb | put | 3198837 | 53.2 | 49.7 | 100 | 313 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7016520 | 3509271 | 42108 | 0.012 | 45322, 0, 0, 0, 0 | 7058628 | 42108 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6838137 | 3420206 | 41774 | 0.012 | 45032, 0, 0, 0, 0 | 6879911 | 41774 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7031412 | 3516623 | 43538 | 0.012 | 46925, 0, 0, 0, 0 | 7074950 | 43538 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 399193 | 43.2% | 11 → 11 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 524618 | 56.8% | 6 → 6 | 0 | 0 | 0 |
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
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 164260 | 100.0% | 7 → 7 | 0 | 0 | 0 |
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
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 0 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-c | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 1 → 1 | 0 | 0 | 0 |
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
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 20849 | 13.9% | 2 → 2 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 13481 | 9.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 39812 | 26.5% | 2 → 2 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 41523 | 27.7% | 4 → 4 | 0 | 0 | 0 |
| ycsb-d | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 34382 | 22.9% | 1 → 2 | 1 | 0 | 0 |
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
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 14575 | 29.2% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 7043 | 14.1% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 15092 | 30.3% | 2 → 3 | 1 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 7507 | 15.1% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 5613 | 11.3% | 1 → 1 | 0 | 0 | 0 |
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
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 396192 | 79.2% | 1 → 7 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 21257 | 4.2% | 2 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 36525 | 7.3% | 1 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 46321 | 9.3% | 3 → 0 | 0 | 0 | 3 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 32955 | 16.5% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 54457 | 27.2% | 5 → 5 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 36605 | 18.3% | 5 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 41416 | 20.7% | 10 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 33670 | 16.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 81 | 0.0% | 3 → 0 | 0 | 0 | 3 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 52 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 278 | 0.1% | 4 → 3 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 24 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 197 | 0.1% | 4 → 3 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 87 | 0.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 97 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 28 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 73 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 458272 | 48.1% | 10 → 10 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 495260 | 51.9% | 11 → 11 | 0 | 0 | 0 |
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
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 520467 | 6.6% | 2 → 4 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 573745 | 7.3% | 1 → 8 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 567406 | 7.2% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 570247 | 7.2% | 1 → 8 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 568339 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 582608 | 7.4% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 428656 | 5.4% | 1 → 0 | 1 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 503742 | 6.4% | 3 → 1 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 444229 | 5.6% | 3 → 1 | 0 | 0 | 3 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 452194 | 5.7% | 2 → 0 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 468824 | 5.9% | 2 → 0 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 399862 | 5.1% | 2 → 0 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 430934 | 5.5% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 467812 | 5.9% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 476565 | 6.0% | 1 → 0 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 442942 | 5.6% | 1 → 0 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 0 | 1732276 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 1 | 1759058 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 2 | 1735706 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 3 | 1758694 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 4 | 1738520 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 5 | 1759436 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 6 | 1740494 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 7 | 1761872 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 8 | 1731450 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 9 | 1767430 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 10 | 1742496 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 11 | 1763762 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 12 | 1737946 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 13 | 1771014 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 14 | 1733634 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on presplit inline in-flight=16 pinned | 15 | 1766212 | 6.3% | 1 → 1 | 0 | 0 | 0 |
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
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 2385 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 15353 | 7.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 13640 | 6.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 17896 | 8.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 7000 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 6681 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 19794 | 9.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 11000 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 23040 | 11.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 6806 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 7392 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 21802 | 10.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 12741 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 5630 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 24103 | 12.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 4737 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 84579 | 11.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 12809 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 77603 | 10.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 71712 | 9.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 53203 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 16556 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 74062 | 9.8% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 31125 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 26451 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 11365 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 85631 | 11.3% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 41478 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 16729 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 61605 | 8.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 44746 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 46457 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 0 | 156447 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 1 | 132521 | 5.8% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 2 | 126268 | 5.6% | 1 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 3 | 124187 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 4 | 169077 | 7.5% | 1 → 1 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 5 | 173314 | 7.6% | 2 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 6 | 167299 | 7.4% | 1 → 2 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 7 | 128635 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 8 | 183748 | 8.1% | 2 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 9 | 129657 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 10 | 115646 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 11 | 128966 | 5.7% | 1 → 1 | 0 | 0 | 1 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 12 | 117649 | 5.2% | 1 → 2 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 13 | 104362 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 14 | 167291 | 7.4% | 1 → 2 | 1 | 0 | 0 |
| group-commit | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync tablets=on | 15 | 143029 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 439192 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 439889 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 437256 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 439496 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 438067 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 438922 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 438412 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 437511 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 438254 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 438306 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 437822 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 438638 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 439623 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 437785 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 437844 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 439503 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 428131 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 428683 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 426211 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 428402 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 426842 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 427795 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 427442 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 426336 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 426944 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 427157 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 426653 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 427382 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 428398 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 426581 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 426967 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 428213 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 0 | 440134 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1 | 440781 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 2 | 438196 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 3 | 440476 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 4 | 438975 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 5 | 439842 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6 | 439352 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7 | 438413 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 8 | 439153 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 9 | 439247 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 10 | 438750 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 11 | 439586 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 12 | 440552 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 13 | 438751 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 14 | 438764 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 15 | 440440 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 2 | 69.6 | 7 | 7 | 2 (10) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-b | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-c | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| ycsb-d | pigeonhole | 1 | 0 | 0.0 | 14 | 3 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.04 | 0.04 | 0.00 | 0 (0) | 0 |
| ycsb-e | pigeonhole | 1 | 0 | 0.0 | 3 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 0 (0) | 0 |
| ycsb-f | pigeonhole | 1 | 0 | 0.0 | 9 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.03 | 0.03 | 0.50 | 0 (0) | 0 |
| sparse-wide | pigeonhole | 1 | 0 | 0.0 | 7 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.20 | 0.20 | 0.00 | 0 (0) | 0 |
| time-series-ttl | pigeonhole | 1 | 2 | 96.6 | 6 | 8 | 1 (6) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
| adjacency | pigeonhole | 1 | 0 | 0.0 | 1 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.08 | 0.08 | 0.00 | 0 (0) | 0 |
| skewed-multi-shard | pigeonhole | 4 | 0 | 0.0 | 93 | 24 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.00 | 125 (0) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 1133 | 282 | 0 (0) | 0 | 0 | 1 | 0.2 | 0.00 | 0.09 | 0.00 | 335701 (65308) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 1062 | 264 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.10 | 0.00 | 3068586 (742689) | 0 |
| group-commit | pigeonhole | 1 | 0 | 0.0 | 4 | 4 | 0 (0) | 0 | 0 | 1 | 0.1 | 1.91 | 1.96 | 1.00 | 0 (0) | 0 |
| group-commit | pigeonhole | 4 | 0 | 0.0 | 30 | 4 | 0 (0) | 0 | 0 | 5 | 0.4 | 0.92 | 1.39 | 1.00 | 2200 (22) | 0 |
| group-commit | pigeonhole | 16 | 0 | 0.0 | 93 | 25 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.83 | 1.00 | 49762 (5922) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 3 | 5.3 | 0.00 | 0.15 | 0.14 | 993136 (653354) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 153 | 32 | 0 (0) | 0 | 0 | 5 | 8.8 | 0.00 | 0.16 | 0.14 | 925884 (581398) | 0 |
| cross-shard-contention | pigeonhole | 32 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 4 | 3.2 | 0.00 | 0.15 | 0.13 | 900882 (581165) | 0 |

**Durable group commit (D213):** 16 shards on 16 cores, the table split over every shard, 16 commits in flight per shard: 10.65M cells/s (665.7K per core), commit p50 71.7 µs, p99 137 µs (target > 1M cells/s, p99 < 200 µs); the busiest shard took 6% of the commits; 0 tablet changes while measuring; inside the engine p50 65.5 µs, p99 98.3 µs, p99.9 786 µs; 0 WAL rollover blocks, 0 inline WAL syncs — pass.

**Durable group commit with the shard threads unpinned (reported, not judged; D235):** 9.71M cells/s, commit p50 78.3 µs, p99 170 µs.
