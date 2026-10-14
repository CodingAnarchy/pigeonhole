**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.98 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 6613450 | 16 | 106.3K | 13.6 | 1384 | 2212 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2741363 | 16 | 32.4K | 24.6 | 3539 | 4456 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4096956 | 16 | 85.8K | 13.8 | 3162 | 33292 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3648 MiB | 0 | 326211 | 5.70 | 30.6 | 46.1 | 3639633 | 7.29 | 786 | 1524 |
| sparse-wide-deletes | rocksdb | 2153 MiB | 0 | 229075 | 20.0 | 3490 | 4227 | 1414971 | 23.8 | 3572 | 4489 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 273686 | 8.89 | 42.8 | 75.3 | 2183543 | 8.00 | 40.2 | 73.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 165593 | 37.0 | 12.0 | 532 | 1925 |
| sparse-wide-deletes | pigeonhole | get | 2643058 | 9.02 | 4.89 | 62.0 | 138 |
| sparse-wide-deletes | pigeonhole | put | 992989 | 38.2 | 13.3 | 537 | 1884 |
| sparse-wide-deletes | pigeonhole | row delete | 165697 | 40.0 | 14.8 | 541 | 1933 |
| sparse-wide-deletes | pigeonhole | row read | 1322786 | 81.8 | 18.7 | 1155 | 1802 |
| sparse-wide-deletes | pigeonhole | scan | 1323327 | 608 | 532 | 1860 | 2720 |
| sparse-wide-deletes | rocksdb | column delete | 68582 | 10.9 | 9.34 | 29.3 | 47.6 |
| sparse-wide-deletes | rocksdb | get | 1094962 | 557 | 11.7 | 3490 | 4194 |
| sparse-wide-deletes | rocksdb | put | 411703 | 13.1 | 11.6 | 31.9 | 56.3 |
| sparse-wide-deletes | rocksdb | row delete | 68857 | 14.9 | 12.9 | 36.4 | 60.9 |
| sparse-wide-deletes | rocksdb | row read | 549084 | 649 | 63.7 | 3736 | 4719 |
| sparse-wide-deletes | rocksdb | scan | 548175 | 685 | 108 | 3752 | 4719 |
| sparse-wide-deletes | sqlite-eav | column delete | 102484 | 817 | 11.6 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | get | 1636949 | 10.4 | 7.46 | 39.2 | 72.7 |
| sparse-wide-deletes | sqlite-eav | put | 615510 | 849 | 26.8 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 102832 | 872 | 28.7 | 18350 | 53740 |
| sparse-wide-deletes | sqlite-eav | row read | 820280 | 12.4 | 9.60 | 43.0 | 75.3 |
| sparse-wide-deletes | sqlite-eav | scan | 818901 | 37.7 | 34.0 | 86.5 | 120 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 589905 | 44.5% | 16 → 16 | 0 | 1 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 734362 | 55.5% | 7 → 9 | 1 | 0 | 0 |
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
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 12 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 12 | 1254.5 | 21 | 10 | 3 (107) | 0 | 0 | 0 | 0.0 | 0.13 | 0.13 | 0.04 | 703 (0) | 0 |
