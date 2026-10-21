**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 7.92 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3586796 | 16 | 26.8K | 14.0 | 5800 | 10289 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 3263794 | 16 | 24.8K | 7.20 | 13894 | 18088 |
| time-series-range | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1470430 | 16 | 10.9K | 79.9 | 29098 | 48234 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 768 MiB | 0 | 582162 | 3.52 | 11.3 | 16.1 | 134147 | 3.41 | 11.2 | 14.5 |
| time-series-range | rocksdb | 295 MiB | 0 | 530948 | 2.65 | 9.86 | 16.1 | 120878 | 2.45 | 8.64 | 11.5 |
| time-series-range | sqlite-eav | 333 MiB | 0 | 243829 | 25.9 | 422 | 582 | 49266 | 28.2 | 446 | 594 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 1435920 | 1473 | 926 | 7668 | 11534 |
| time-series-range | pigeonhole | get | 716309 | 4.14 | 3.49 | 11.3 | 15.7 |
| time-series-range | pigeonhole | put at | 1434567 | 12.1 | 9.41 | 21.9 | 84.5 |
| time-series-range | rocksdb | filtered scan | 1306041 | 1607 | 131 | 16515 | 19005 |
| time-series-range | rocksdb | get | 651826 | 3.11 | 2.62 | 9.60 | 15.3 |
| time-series-range | rocksdb | put at | 1305927 | 5.72 | 4.77 | 14.5 | 20.5 |
| time-series-range | sqlite-eav | filtered scan | 588570 | 2881 | 247 | 35914 | 48234 |
| time-series-range | sqlite-eav | get | 293095 | 47.8 | 26.2 | 428 | 586 |
| time-series-range | sqlite-eav | put at | 588765 | 743 | 69.6 | 9175 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 439682 | 30.6% | 4 → 6 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 994885 | 69.4% | 14 → 12 | 0 | 0 | 2 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 4 | 151.7 | 12 | 5 | 2 (15) | 0 | 0 | 0 | 0.0 | 0.19 | 0.19 | 0.00 | 60 (0) | 0 |
