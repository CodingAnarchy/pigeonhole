**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 10.04 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 3031296 | 16 | 22.8K | 20.5 | 6947 | 10093 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 3204741 | 16 | 24.3K | 7.10 | 14221 | 18481 |
| time-series-range | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1497795 | 16 | 11.1K | 77.8 | 27525 | 46924 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 493871 | 3.28 | 11.2 | 16.1 | 111374 | 3.09 | 10.8 | 14.2 |
| time-series-range | rocksdb | 292 MiB | 0 | 521616 | 2.65 | 9.98 | 16.4 | 118445 | 2.43 | 8.64 | 11.5 |
| time-series-range | sqlite-eav | 335 MiB | 0 | 248037 | 25.3 | 438 | 598 | 50290 | 27.4 | 463 | 623 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 1212850 | 1730 | 1114 | 8454 | 10945 |
| time-series-range | pigeonhole | get | 605245 | 3.91 | 3.25 | 11.1 | 15.6 |
| time-series-range | pigeonhole | put at | 1213201 | 22.5 | 17.3 | 37.6 | 1663 |
| time-series-range | rocksdb | filtered scan | 1282242 | 1635 | 132 | 16712 | 19530 |
| time-series-range | rocksdb | get | 640061 | 3.12 | 2.61 | 9.73 | 15.7 |
| time-series-range | rocksdb | put at | 1282438 | 5.63 | 4.64 | 14.1 | 19.8 |
| time-series-range | sqlite-eav | filtered scan | 599682 | 2820 | 248 | 35389 | 46924 |
| time-series-range | sqlite-eav | get | 298327 | 47.3 | 25.7 | 442 | 602 |
| time-series-range | sqlite-eav | put at | 599786 | 736 | 66.6 | 9044 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 54979 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 20893 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 92129 | 7.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 58313 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 89163 | 7.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 50281 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 111181 | 9.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 87567 | 7.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 178309 | 14.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 110782 | 9.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 54587 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 71665 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 52268 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 74659 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 52210 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 54215 | 4.5% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 5 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.38 | 0.38 | 0.00 | 69 (1) | 0 |
