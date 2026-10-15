**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.45 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 12165610 | 16 | 69.2K | 18.7 | 2785 | 4391 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 12702845 | 16 | 75.8K | 15.6 | 3031 | 4653 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4445431 | 16 | 41.2K | 34.6 | 3572 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3904 MiB | 0 | 385815 | 7.26 | 63.0 | 143 | 6910078 | 12.7 | 2605 | 4178 |
| sparse-wide | rocksdb | 2503 MiB | 0 | 389452 | 14.4 | 35.6 | 44.5 | 7228546 | 15.6 | 2884 | 4522 |
| sparse-wide | sqlite-eav | 2874 MiB | 0 | 282951 | 14.3 | 73.2 | 118 | 2384241 | 18.7 | 1876 | 3015 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 4862692 | 10.1 | 7.46 | 38.7 | 93.2 |
| sparse-wide | pigeonhole | put | 2433944 | 41.0 | 16.8 | 823 | 2408 |
| sparse-wide | pigeonhole | row read | 2433201 | 434 | 71.2 | 3326 | 4784 |
| sparse-wide | pigeonhole | scan | 2435773 | 642 | 360 | 3539 | 5046 |
| sparse-wide | rocksdb | get | 5077479 | 10.3 | 9.28 | 30.5 | 38.7 |
| sparse-wide | rocksdb | put | 2541653 | 7.51 | 7.04 | 16.5 | 21.8 |
| sparse-wide | rocksdb | row read | 2540519 | 488 | 56.6 | 3719 | 4850 |
| sparse-wide | rocksdb | scan | 2543194 | 538 | 99.3 | 3817 | 4948 |
| sparse-wide | sqlite-eav | get | 1778068 | 18.9 | 14.0 | 75.8 | 124 |
| sparse-wide | sqlite-eav | put | 888328 | 1244 | 66.6 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 889124 | 282 | 41.7 | 2408 | 3555 |
| sparse-wide | sqlite-eav | scan | 889911 | 347 | 89.1 | 2654 | 3801 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 457075 | 18.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 616345 | 25.3% | 9 → 13 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 588121 | 24.2% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 772144 | 31.7% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 12 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 208 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 20 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 14 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 5 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 1 | 39.8 | 74 | 30 | 8 (94) | 0 | 0 | 0 | 0.0 | 0.18 | 0.18 | 0.01 | 509 (5) | 1432 |
