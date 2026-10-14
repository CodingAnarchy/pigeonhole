**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 9.28 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1674066 | 16 | 18.0K | 25.3 | 6324 | 9961 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 3229375 | 16 | 24.5K | 7.20 | 14025 | 18219 |
| time-series-range | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1507766 | 16 | 11.2K | 76.3 | 28967 | 47710 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 384 MiB | 0 | 276534 | 3.06 | 10.6 | 17.9 | 57092 | 2.80 | 10.1 | 13.5 |
| time-series-range | rocksdb | 293 MiB | 0 | 525633 | 2.69 | 9.79 | 16.5 | 119402 | 2.45 | 8.70 | 11.6 |
| time-series-range | sqlite-eav | 336 MiB | 0 | 249730 | 24.7 | 453 | 614 | 50644 | 26.6 | 475 | 623 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 669917 | 1966 | 1745 | 8094 | 10617 |
| time-series-range | pigeonhole | get | 333626 | 3.62 | 3.01 | 10.6 | 17.0 |
| time-series-range | pigeonhole | put at | 670523 | 41.5 | 18.9 | 848 | 2933 |
| time-series-range | rocksdb | filtered scan | 1292042 | 1624 | 132 | 16712 | 19137 |
| time-series-range | rocksdb | get | 645035 | 3.13 | 2.64 | 9.60 | 15.6 |
| time-series-range | rocksdb | put at | 1292298 | 5.71 | 4.74 | 14.4 | 20.4 |
| time-series-range | sqlite-eav | filtered scan | 603568 | 2793 | 243 | 35914 | 47186 |
| time-series-range | sqlite-eav | get | 300374 | 46.8 | 25.1 | 457 | 614 |
| time-series-range | sqlite-eav | put at | 603824 | 738 | 64.8 | 8978 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 30192 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 18741 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 27461 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 27764 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 76890 | 11.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 23661 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 80531 | 12.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 63070 | 9.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 71430 | 10.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 43893 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 10924 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 71146 | 10.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 19131 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 43896 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 29367 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 32426 | 4.8% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.40 | 0.39 | 0.23 | 1264 (5) | 0 |
