**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 9.33 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1672721 | 16 | 16.7K | 21.4 | 5931 | 9372 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 3268566 | 16 | 24.9K | 7.13 | 13631 | 17826 |
| time-series-range | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1530987 | 16 | 11.4K | 74.8 | 27918 | 48759 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 276299 | 3.15 | 10.9 | 17.8 | 57038 | 2.88 | 10.4 | 13.4 |
| time-series-range | rocksdb | 295 MiB | 0 | 531756 | 2.64 | 9.98 | 16.6 | 120995 | 2.42 | 8.83 | 11.6 |
| time-series-range | sqlite-eav | 336 MiB | 0 | 253466 | 24.6 | 438 | 598 | 51559 | 26.5 | 455 | 623 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 669381 | 1925 | 1720 | 7668 | 10027 |
| time-series-range | pigeonhole | get | 333337 | 3.76 | 3.09 | 10.9 | 17.0 |
| time-series-range | pigeonhole | put at | 670003 | 22.7 | 17.5 | 39.7 | 1556 |
| time-series-range | rocksdb | filtered scan | 1307768 | 1601 | 131 | 16384 | 18743 |
| time-series-range | rocksdb | get | 652751 | 3.13 | 2.61 | 9.73 | 15.7 |
| time-series-range | rocksdb | put at | 1308047 | 5.65 | 4.70 | 14.4 | 20.4 |
| time-series-range | sqlite-eav | filtered scan | 612860 | 2761 | 239 | 35127 | 48497 |
| time-series-range | sqlite-eav | get | 305025 | 45.2 | 25.0 | 442 | 602 |
| time-series-range | sqlite-eav | put at | 613102 | 719 | 63.7 | 8847 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 28482 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 15048 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 30663 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 53924 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 45920 | 6.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 98437 | 14.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 73017 | 10.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 39234 | 5.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 42993 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 55160 | 8.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 52573 | 7.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 19180 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 33270 | 5.0% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 27920 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 21865 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 32317 | 4.8% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 1 | 2 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.40 | 0.40 | 0.00 | 20 (1) | 0 |
