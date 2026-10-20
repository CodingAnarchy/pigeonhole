**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.57 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 10170155 | 16 | 72.1K | 17.3 | 2638 | 4080 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 12543113 | 16 | 73.9K | 15.6 | 3129 | 4719 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4582639 | 16 | 41.4K | 34.0 | 3506 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3776 MiB | 0 | 369928 | 7.17 | 57.1 | 133 | 5728727 | 11.8 | 2490 | 3883 |
| sparse-wide | rocksdb | 2491 MiB | 0 | 388448 | 14.2 | 35.6 | 44.5 | 7133709 | 15.5 | 2966 | 4588 |
| sparse-wide | sqlite-eav | 2880 MiB | 0 | 286271 | 14.4 | 70.7 | 115 | 2463131 | 18.9 | 1909 | 2998 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 4065154 | 9.45 | 7.13 | 34.0 | 80.4 |
| sparse-wide | pigeonhole | put | 2035435 | 30.3 | 15.0 | 498 | 2064 |
| sparse-wide | pigeonhole | row read | 2033501 | 401 | 63.2 | 3146 | 4391 |
| sparse-wide | pigeonhole | scan | 2036065 | 610 | 348 | 3326 | 4620 |
| sparse-wide | rocksdb | get | 5013622 | 10.3 | 9.28 | 30.7 | 38.9 |
| sparse-wide | rocksdb | put | 2509627 | 7.59 | 7.07 | 16.5 | 21.9 |
| sparse-wide | rocksdb | row read | 2508535 | 499 | 56.3 | 3768 | 4915 |
| sparse-wide | rocksdb | scan | 2511329 | 553 | 99.8 | 3883 | 5014 |
| sparse-wide | sqlite-eav | get | 1832762 | 18.7 | 14.1 | 73.7 | 118 |
| sparse-wide | sqlite-eav | put | 915767 | 1234 | 65.5 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 916640 | 287 | 41.7 | 2425 | 3457 |
| sparse-wide | sqlite-eav | scan | 917470 | 348 | 88.1 | 2654 | 3719 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 360482 | 17.7% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 437554 | 21.5% | 3 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 413992 | 20.3% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 801702 | 39.4% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 21606 | 1.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 12 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 34 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 16 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 14 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 7 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 1 | 4.7 | 66 | 28 | 7 (82) | 0 | 0 | 0 | 0.0 | 0.13 | 0.13 | 0.01 | 356 (1) | 0 |
