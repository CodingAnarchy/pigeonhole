**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.44 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 9192641 | 16 | 81.6K | 14.9 | 2245 | 3490 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 13069274 | 16 | 76.8K | 15.7 | 2982 | 4555 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4512398 | 16 | 41.4K | 34.6 | 3572 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3968 MiB | 0 | 360269 | 6.27 | 49.9 | 105 | 5152016 | 11.3 | 2089 | 3342 |
| sparse-wide | rocksdb | 2511 MiB | 0 | 391797 | 14.3 | 35.6 | 44.5 | 7446285 | 15.6 | 2834 | 4424 |
| sparse-wide | sqlite-eav | 2877 MiB | 0 | 284594 | 14.6 | 71.2 | 117 | 2422696 | 19.1 | 1892 | 3080 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3674352 | 8.65 | 6.33 | 30.0 | 60.7 |
| sparse-wide | pigeonhole | put | 1840070 | 22.0 | 12.2 | 176 | 1655 |
| sparse-wide | pigeonhole | row read | 1837933 | 340 | 52.7 | 2687 | 3883 |
| sparse-wide | pigeonhole | scan | 1840286 | 557 | 342 | 2818 | 3998 |
| sparse-wide | rocksdb | get | 5223901 | 10.4 | 9.34 | 30.7 | 38.9 |
| sparse-wide | rocksdb | put | 2615010 | 7.72 | 7.13 | 17.0 | 23.3 |
| sparse-wide | rocksdb | row read | 2614181 | 480 | 56.6 | 3621 | 4751 |
| sparse-wide | rocksdb | scan | 2616182 | 532 | 98.8 | 3736 | 4850 |
| sparse-wide | sqlite-eav | get | 1804709 | 18.9 | 14.2 | 74.8 | 121 |
| sparse-wide | sqlite-eav | put | 901850 | 1236 | 66.0 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 902581 | 285 | 42.0 | 2425 | 3621 |
| sparse-wide | sqlite-eav | scan | 903258 | 350 | 90.1 | 2687 | 3899 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1656389 | 90.0% | 1 → 12 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 83304 | 4.5% | 3 → 0 | 0 | 0 | 5 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 51011 | 2.8% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 49366 | 2.7% | 3 → 0 | 0 | 0 | 3 |
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

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 17 | 670.5 | 55 | 28 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 0.11 | 0.01 | 87 (0) | 0 |
