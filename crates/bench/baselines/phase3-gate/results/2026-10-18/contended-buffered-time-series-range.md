**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 10.52 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3180312 | 16 | 23.9K | 20.9 | 6390 | 9699 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 3254943 | 16 | 24.7K | 7.10 | 13500 | 18088 |
| time-series-range | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1483765 | 16 | 11.0K | 78.8 | 27656 | 48497 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 517818 | 3.33 | 11.2 | 16.2 | 117348 | 3.15 | 10.9 | 14.4 |
| time-series-range | rocksdb | 293 MiB | 0 | 529498 | 2.65 | 9.73 | 16.6 | 120527 | 2.42 | 8.64 | 11.3 |
| time-series-range | sqlite-eav | 334 MiB | 0 | 245880 | 25.6 | 432 | 598 | 49760 | 27.8 | 453 | 614 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 1272323 | 1647 | 1106 | 7766 | 10682 |
| time-series-range | pigeonhole | get | 635166 | 3.94 | 3.29 | 11.1 | 15.7 |
| time-series-range | pigeonhole | put at | 1272823 | 21.3 | 17.3 | 37.1 | 1155 |
| time-series-range | rocksdb | filtered scan | 1302386 | 1613 | 132 | 16515 | 18874 |
| time-series-range | rocksdb | get | 650025 | 3.11 | 2.61 | 9.47 | 15.9 |
| time-series-range | rocksdb | put at | 1302532 | 5.63 | 4.70 | 14.4 | 20.7 |
| time-series-range | sqlite-eav | filtered scan | 593920 | 2852 | 248 | 37487 | 48497 |
| time-series-range | sqlite-eav | get | 295640 | 47.5 | 26.0 | 438 | 598 |
| time-series-range | sqlite-eav | put at | 594205 | 738 | 68.1 | 8913 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 76724 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 40319 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 58513 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 74546 | 5.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 52295 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 74770 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 171069 | 13.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 109044 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 132560 | 10.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 117207 | 9.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 49960 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 86471 | 6.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 51623 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 66147 | 5.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 54738 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 56837 | 4.5% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 4 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.40 | 0.40 | 0.00 | 93 (0) | 0 |
