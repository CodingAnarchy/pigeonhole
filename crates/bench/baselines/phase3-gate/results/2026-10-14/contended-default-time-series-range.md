**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 9.17 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 3565754 | 16 | 27.1K | 67.1 | 5571 | 8913 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2861479 | 16 | 21.5K | 91.6 | 13435 | 22413 |
| time-series-range | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1350677 | 16 | 12.6K | 75.8 | 19268 | 79167 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 579157 | 3.31 | 10.9 | 15.7 | 133270 | 3.21 | 10.8 | 14.0 |
| time-series-range | rocksdb | 272 MiB | 0 | 467063 | 3.60 | 12.9 | 21.4 | 104366 | 3.26 | 11.2 | 14.7 |
| time-series-range | sqlite-eav | 319 MiB | 0 | 224497 | 13.4 | 301 | 479 | 44749 | 14.4 | 319 | 479 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 1426611 | 1386 | 827 | 6947 | 9896 |
| time-series-range | pigeonhole | get | 712427 | 3.92 | 3.29 | 10.9 | 15.3 |
| time-series-range | pigeonhole | put at | 1426716 | 89.4 | 59.4 | 901 | 2998 |
| time-series-range | rocksdb | filtered scan | 1144967 | 1644 | 139 | 16122 | 24510 |
| time-series-range | rocksdb | get | 571429 | 4.25 | 3.54 | 12.5 | 20.7 |
| time-series-range | rocksdb | put at | 1145083 | 215 | 124 | 3015 | 4489 |
| time-series-range | sqlite-eav | filtered scan | 540367 | 1322 | 128 | 19268 | 33292 |
| time-series-range | sqlite-eav | get | 269246 | 28.5 | 13.6 | 303 | 479 |
| time-series-range | sqlite-eav | put at | 541064 | 1753 | 99.3 | 33817 | 128975 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 117376 | 8.2% | 1 → 5 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 795977 | 55.8% | 0 → 10 | 2 | 0 | 3 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 510883 | 35.8% | 2 → 5 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 923 | 0.1% | 2 → 0 | 0 | 0 | 2 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 315 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 405 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 312 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 525 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 3 | 34.2 | 27 | 7 | 1 (6) | 0 | 0 | 0 | 0.0 | 0.49 | 0.58 | 0.40 | 435 (7) | 0 |
