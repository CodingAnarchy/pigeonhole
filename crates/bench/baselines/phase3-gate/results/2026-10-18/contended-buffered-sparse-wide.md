**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.48 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 8863614 | 16 | 79.1K | 17.3 | 2261 | 3555 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 12957919 | 16 | 76.0K | 15.5 | 3015 | 4653 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4643309 | 16 | 40.6K | 35.1 | 3572 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3584 MiB | 0 | 356701 | 6.88 | 50.9 | 104 | 4958569 | 11.1 | 2163 | 3408 |
| sparse-wide | rocksdb | 2521 MiB | 0 | 391105 | 14.3 | 35.3 | 44.3 | 7380021 | 15.4 | 2867 | 4522 |
| sparse-wide | sqlite-eav | 2882 MiB | 0 | 287767 | 14.5 | 74.2 | 120 | 2498038 | 19.1 | 1917 | 2966 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3542840 | 8.81 | 6.66 | 30.5 | 66.6 |
| sparse-wide | pigeonhole | put | 1774169 | 43.5 | 16.1 | 856 | 2310 |
| sparse-wide | pigeonhole | row read | 1772430 | 351 | 54.3 | 2736 | 3883 |
| sparse-wide | pigeonhole | scan | 1774175 | 537 | 324 | 2802 | 4014 |
| sparse-wide | rocksdb | get | 5179352 | 10.2 | 9.21 | 30.2 | 38.4 |
| sparse-wide | rocksdb | put | 2592675 | 7.67 | 7.13 | 16.9 | 23.0 |
| sparse-wide | rocksdb | row read | 2591774 | 485 | 56.1 | 3654 | 4817 |
| sparse-wide | rocksdb | scan | 2594118 | 538 | 98.3 | 3785 | 4981 |
| sparse-wide | sqlite-eav | get | 1856994 | 19.2 | 14.1 | 76.8 | 123 |
| sparse-wide | sqlite-eav | put | 927941 | 1253 | 66.6 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 928811 | 288 | 42.8 | 2425 | 3408 |
| sparse-wide | sqlite-eav | scan | 929563 | 360 | 91.6 | 2736 | 3719 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 177401 | 10.0% | 10 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 117650 | 6.6% | 2 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 861191 | 48.5% | 7 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 199147 | 11.2% | 3 → 14 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 219485 | 12.4% | 9 → 11 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 24060 | 1.4% | 5 → 0 | 0 | 0 | 5 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 28061 | 1.6% | 6 → 0 | 0 | 0 | 6 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1249 | 0.1% | 5 → 0 | 0 | 0 | 5 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 40428 | 2.3% | 5 → 0 | 0 | 0 | 5 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1270 | 0.1% | 5 → 0 | 0 | 0 | 5 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 1134 | 0.1% | 7 → 0 | 0 | 0 | 7 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 579 | 0.0% | 3 → 1 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 28688 | 1.6% | 3 → 0 | 0 | 0 | 3 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 36266 | 2.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 27728 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 9832 | 0.6% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 8 | 316.2 | 79 | 17 | 2 (22) | 0 | 0 | 0 | 0.0 | 0.17 | 0.17 | 0.02 | 918 (8) | 0 |
