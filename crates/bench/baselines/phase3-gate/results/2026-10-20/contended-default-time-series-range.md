**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 9.33 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 1692888 | 16 | 20.9K | 73.2 | 5276 | 8258 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2869481 | 16 | 21.5K | 92.2 | 13500 | 21758 |
| time-series-range | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1363822 | 16 | 12.7K | 76.3 | 18481 | 79167 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 279614 | 3.63 | 11.4 | 21.9 | 57806 | 3.36 | 10.8 | 14.5 |
| time-series-range | rocksdb | 272 MiB | 0 | 468385 | 3.54 | 13.2 | 21.4 | 104689 | 3.18 | 11.1 | 14.3 |
| time-series-range | sqlite-eav | 319 MiB | 0 | 226608 | 13.9 | 324 | 457 | 45268 | 14.7 | 336 | 459 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 677360 | 1652 | 1253 | 6455 | 9372 |
| time-series-range | pigeonhole | get | 337420 | 4.18 | 3.57 | 11.3 | 21.2 |
| time-series-range | pigeonhole | put at | 678108 | 170 | 64.8 | 2720 | 4047 |
| time-series-range | rocksdb | filtered scan | 1148148 | 1634 | 137 | 16187 | 24117 |
| time-series-range | rocksdb | get | 573074 | 4.23 | 3.47 | 12.7 | 20.9 |
| time-series-range | rocksdb | put at | 1148259 | 221 | 125 | 3080 | 4719 |
| time-series-range | sqlite-eav | filtered scan | 545672 | 1277 | 127 | 16187 | 23724 |
| time-series-range | sqlite-eav | get | 271876 | 28.4 | 14.0 | 326 | 459 |
| time-series-range | sqlite-eav | put at | 546274 | 1765 | 102 | 33817 | 128975 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 25078 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 22697 | 3.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 24619 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 26294 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 42240 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 46686 | 6.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 41736 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 100793 | 14.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 53409 | 7.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 34476 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 55699 | 8.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 44786 | 6.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 49489 | 7.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 32399 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 43923 | 6.5% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 33784 | 5.0% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.73 | 0.75 | 0.40 | 1128 (24) | 0 |
