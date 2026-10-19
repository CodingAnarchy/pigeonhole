**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.96 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 32067046 | 16 | 532.9K | 12.4 | 118 | 173 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 17336785 | 16 | 246.3K | 33.0 | 205 | 238 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2526579 | 16 | 40.7K | 8.38 | 112 | 78643 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 2816 MiB | 0 | 5057854 | 2.83 | 7.36 | 20.2 | 1351612 | 2.88 | 7.90 | 20.0 |
| time-series-ttl | rocksdb | 980 MiB | 0 | 2747034 | 2.51 | 10.8 | 15.7 | 719199 | 2.58 | 11.1 | 15.7 |
| time-series-ttl | sqlite-eav | 406 MiB | 0 | 413701 | 5.70 | 16.8 | 36.6 | 90743 | 5.50 | 16.6 | 38.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 6409466 | 3.17 | 2.85 | 7.46 | 20.1 |
| time-series-ttl | pigeonhole | put at | 12825448 | 62.4 | 54.0 | 142 | 255 |
| time-series-ttl | pigeonhole | scan | 12832132 | 10.6 | 10.1 | 21.4 | 41.7 |
| time-series-ttl | rocksdb | get | 3466233 | 3.37 | 2.53 | 10.8 | 15.7 |
| time-series-ttl | rocksdb | put at | 6934832 | 135 | 131 | 217 | 266 |
| time-series-ttl | rocksdb | scan | 6935720 | 25.8 | 23.7 | 64.8 | 93.7 |
| time-series-ttl | sqlite-eav | get | 504444 | 5.83 | 5.66 | 16.6 | 36.9 |
| time-series-ttl | sqlite-eav | put at | 1011148 | 889 | 39.2 | 3178 | 179306 |
| time-series-ttl | sqlite-eav | scan | 1010987 | 6.97 | 6.72 | 18.4 | 40.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1779937 | 13.9% | 1 → 1 | 19 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 974362 | 7.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 593352 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 581647 | 4.5% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 613853 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 583686 | 4.6% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 804384 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 233707 | 1.8% | 2 → 16 | 0 | 5 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 593467 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 1079272 | 8.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 932425 | 7.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 634440 | 4.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 871765 | 6.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 1102261 | 8.6% | 1 → 1 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 749962 | 5.8% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 696928 | 5.4% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 0 | 0.0 | 141 | 47 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.31 | 0.48 | 0.40 | 75887 (9191) | 38 |
