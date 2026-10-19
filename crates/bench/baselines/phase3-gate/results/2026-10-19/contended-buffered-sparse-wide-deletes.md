**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.23 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 5888141 | 16 | 87.8K | 15.4 | 1827 | 2982 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2727699 | 16 | 33.3K | 25.0 | 3375 | 4227 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4184631 | 16 | 85.6K | 13.9 | 3162 | 33292 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3072 MiB | 0 | 313906 | 6.08 | 31.4 | 48.1 | 3217888 | 7.78 | 1221 | 2277 |
| sparse-wide-deletes | rocksdb | 2155 MiB | 0 | 228549 | 20.5 | 3277 | 3883 | 1407227 | 24.4 | 3408 | 4260 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 276086 | 8.89 | 41.5 | 75.8 | 2233793 | 8.06 | 39.4 | 73.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 147266 | 39.2 | 14.1 | 852 | 2408 |
| sparse-wide-deletes | pigeonhole | get | 2353588 | 11.3 | 5.25 | 92.7 | 210 |
| sparse-wide-deletes | pigeonhole | put | 883518 | 40.6 | 15.6 | 864 | 2408 |
| sparse-wide-deletes | pigeonhole | row delete | 147401 | 42.3 | 17.2 | 848 | 2441 |
| sparse-wide-deletes | pigeonhole | row read | 1178206 | 129 | 19.2 | 1696 | 2687 |
| sparse-wide-deletes | pigeonhole | scan | 1178162 | 703 | 582 | 2523 | 3572 |
| sparse-wide-deletes | rocksdb | column delete | 68263 | 10.9 | 9.28 | 28.8 | 50.2 |
| sparse-wide-deletes | rocksdb | get | 1089509 | 539 | 12.0 | 3310 | 3965 |
| sparse-wide-deletes | rocksdb | put | 409670 | 13.0 | 11.4 | 31.2 | 66.6 |
| sparse-wide-deletes | rocksdb | row delete | 68518 | 14.2 | 12.6 | 34.3 | 62.7 |
| sparse-wide-deletes | rocksdb | row read | 546267 | 631 | 66.6 | 3555 | 4489 |
| sparse-wide-deletes | rocksdb | scan | 545472 | 668 | 110 | 3588 | 4489 |
| sparse-wide-deletes | sqlite-eav | column delete | 104648 | 823 | 11.6 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | get | 1672011 | 10.4 | 7.49 | 38.1 | 72.7 |
| sparse-wide-deletes | sqlite-eav | put | 628535 | 850 | 26.9 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 104921 | 855 | 28.4 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row read | 837868 | 12.4 | 9.73 | 42.2 | 75.3 |
| sparse-wide-deletes | sqlite-eav | scan | 836648 | 38.6 | 35.1 | 86.5 | 119 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 282195 | 24.0% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 241472 | 20.5% | 7 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 419716 | 35.6% | 8 → 8 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 221508 | 18.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 13128 | 1.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 18 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 44 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 73 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 15 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 3 | 176.6 | 29 | 3 | 1 (28) | 0 | 0 | 0 | 0.0 | 0.17 | 0.17 | 0.01 | 342 (1) | 0 |
