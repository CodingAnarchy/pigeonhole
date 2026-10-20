**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.24 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 5783382 | 16 | 108.2K | 11.3 | 1335 | 2023 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2658226 | 16 | 34.1K | 24.7 | 3228 | 4063 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4038448 | 16 | 85.0K | 13.9 | 3162 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3392 MiB | 0 | 311920 | 5.63 | 30.3 | 45.8 | 3157147 | 7.42 | 688 | 1483 |
| sparse-wide-deletes | rocksdb | 2156 MiB | 0 | 225740 | 20.5 | 3178 | 3785 | 1368557 | 24.2 | 3260 | 4096 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 272015 | 8.89 | 41.7 | 75.8 | 2150120 | 8.13 | 40.4 | 73.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 144584 | 14.6 | 7.17 | 73.2 | 1081 |
| sparse-wide-deletes | pigeonhole | get | 2311549 | 9.02 | 4.86 | 60.9 | 133 |
| sparse-wide-deletes | pigeonhole | put | 867575 | 16.0 | 8.64 | 75.3 | 1114 |
| sparse-wide-deletes | pigeonhole | row delete | 144767 | 16.5 | 9.86 | 70.1 | 1049 |
| sparse-wide-deletes | pigeonhole | row read | 1157518 | 79.4 | 18.7 | 1028 | 1876 |
| sparse-wide-deletes | pigeonhole | scan | 1157389 | 615 | 561 | 1753 | 2458 |
| sparse-wide-deletes | rocksdb | column delete | 66519 | 11.0 | 9.34 | 29.3 | 136 |
| sparse-wide-deletes | rocksdb | get | 1061860 | 525 | 12.0 | 3162 | 3817 |
| sparse-wide-deletes | rocksdb | put | 399057 | 13.1 | 11.6 | 31.7 | 142 |
| sparse-wide-deletes | rocksdb | row delete | 66759 | 14.7 | 12.8 | 36.4 | 135 |
| sparse-wide-deletes | rocksdb | row read | 532437 | 615 | 62.7 | 3408 | 4325 |
| sparse-wide-deletes | rocksdb | scan | 531594 | 655 | 109 | 3441 | 4293 |
| sparse-wide-deletes | sqlite-eav | column delete | 101006 | 835 | 11.8 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | get | 1613497 | 10.5 | 7.55 | 39.2 | 73.2 |
| sparse-wide-deletes | sqlite-eav | put | 606796 | 859 | 26.9 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 101325 | 851 | 28.8 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row read | 808638 | 12.5 | 9.79 | 43.0 | 75.8 |
| sparse-wide-deletes | sqlite-eav | scan | 807186 | 38.8 | 35.3 | 87.6 | 120 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 426362 | 36.9% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 730564 | 63.1% | 9 → 9 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 10 | 756.1 | 20 | 10 | 2 (89) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 97 (1) | 1 |
