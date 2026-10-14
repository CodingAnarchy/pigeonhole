**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.59 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 9477972 | 16 | 71.5K | 17.7 | 2572 | 4014 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 12035465 | 16 | 72.3K | 15.5 | 3211 | 4817 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4501495 | 16 | 41.0K | 34.6 | 3637 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3776 MiB | 0 | 363250 | 6.82 | 68.6 | 164 | 5320339 | 11.9 | 2425 | 3834 |
| sparse-wide | rocksdb | 2476 MiB | 0 | 385007 | 14.1 | 35.3 | 44.3 | 6832582 | 15.4 | 3064 | 4719 |
| sparse-wide | sqlite-eav | 2877 MiB | 0 | 284329 | 14.3 | 72.7 | 119 | 2416492 | 18.9 | 1892 | 3080 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3788624 | 9.71 | 6.88 | 40.4 | 120 |
| sparse-wide | pigeonhole | put | 1897140 | 40.3 | 15.5 | 532 | 1958 |
| sparse-wide | pigeonhole | row read | 1894965 | 385 | 64.3 | 3080 | 4358 |
| sparse-wide | pigeonhole | scan | 1897243 | 589 | 350 | 3228 | 4555 |
| sparse-wide | rocksdb | get | 4810331 | 10.2 | 9.15 | 30.3 | 38.4 |
| sparse-wide | rocksdb | put | 2408155 | 7.60 | 7.13 | 16.5 | 21.6 |
| sparse-wide | rocksdb | row read | 2407258 | 510 | 56.6 | 3867 | 4981 |
| sparse-wide | rocksdb | scan | 2409721 | 566 | 102 | 3998 | 5112 |
| sparse-wide | sqlite-eav | get | 1800363 | 19.1 | 14.1 | 77.3 | 127 |
| sparse-wide | sqlite-eav | put | 899593 | 1263 | 68.6 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 900458 | 283 | 42.0 | 2425 | 3621 |
| sparse-wide | sqlite-eav | scan | 901081 | 342 | 88.1 | 2671 | 3867 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 628793 | 33.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1268337 | 66.9% | 9 → 10 | 0 | 0 | 0 |
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
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 10 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 16 | 780.4 | 48 | 21 | 4 (30) | 0 | 0 | 0 | 0.0 | 0.14 | 0.14 | 0.06 | 1349 (6) | 2 |
