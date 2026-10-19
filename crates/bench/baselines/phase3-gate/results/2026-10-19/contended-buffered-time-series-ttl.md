**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.61 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 80135536 | 16 | 1.27M | 9.28 | 42.2 | 79.9 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 59424157 | 16 | 378.1K | 13.1 | 319 | 387 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4946270 | 16 | 75.6K | 9.15 | 3146 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 6336 MiB | 0 | 12612441 | 3.36 | 12.6 | 21.6 | 3414933 | 3.44 | 13.1 | 22.8 |
| time-series-ttl | rocksdb | 3055 MiB | 0 | 9357704 | 4.96 | 21.6 | 28.4 | 2526495 | 5.25 | 21.2 | 26.9 |
| time-series-ttl | sqlite-eav | 621 MiB | 0 | 797824 | 6.46 | 25.3 | 44.3 | 190483 | 6.43 | 25.6 | 45.3 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 16027374 | 3.96 | 3.38 | 12.7 | 21.9 |
| time-series-ttl | pigeonhole | put at | 32055321 | 10.7 | 7.49 | 23.2 | 93.2 |
| time-series-ttl | pigeonhole | scan | 32052841 | 17.5 | 15.3 | 49.4 | 82.4 |
| time-series-ttl | rocksdb | get | 11884199 | 6.79 | 5.02 | 21.6 | 28.2 |
| time-series-ttl | rocksdb | put at | 23771282 | 10.1 | 9.60 | 20.2 | 110 |
| time-series-ttl | rocksdb | scan | 23768676 | 92.1 | 71.7 | 352 | 403 |
| time-series-ttl | sqlite-eav | get | 988307 | 7.38 | 6.46 | 25.3 | 44.3 |
| time-series-ttl | sqlite-eav | put at | 1979128 | 502 | 14.0 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1978835 | 8.60 | 7.62 | 27.5 | 46.6 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 3458055 | 10.8% | 2 → 6 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 5453775 | 17.0% | 1 → 3 | 0 | 0 | 9 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 3334537 | 10.4% | 2 → 3 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 3430660 | 10.7% | 2 → 5 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 3397979 | 10.6% | 1 → 5 | 2 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1996016 | 6.2% | 2 → 5 | 0 | 12 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 3074424 | 9.6% | 2 → 4 | 1 | 0 | 6 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 988365 | 3.1% | 2 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1800046 | 5.6% | 1 → 0 | 14 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 821777 | 2.6% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 717016 | 2.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 870887 | 2.7% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 707799 | 2.2% | 1 → 0 | 1 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 757469 | 2.4% | 1 → 0 | 1 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 677734 | 2.1% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 568782 | 1.8% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 23 | 1489.8 | 355 | 133 | 17 (42) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 13371 (1163) | 17 |
