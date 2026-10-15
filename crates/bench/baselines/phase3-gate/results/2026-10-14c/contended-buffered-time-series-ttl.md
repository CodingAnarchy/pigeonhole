**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.59 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 85610203 | 16 | 1.38M | 8.64 | 42.2 | 72.2 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 59575267 | 16 | 372.7K | 13.2 | 330 | 414 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4545010 | 16 | 73.2K | 9.28 | 3146 | 33817 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 6400 MiB | 0 | 13472489 | 3.39 | 12.9 | 21.8 | 3650155 | 3.46 | 13.2 | 22.5 |
| time-series-ttl | rocksdb | 3063 MiB | 0 | 9381160 | 5.41 | 23.3 | 31.6 | 2533198 | 5.63 | 22.7 | 30.0 |
| time-series-ttl | sqlite-eav | 586 MiB | 0 | 733917 | 6.53 | 26.5 | 45.3 | 173800 | 6.46 | 26.4 | 45.8 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 17122644 | 4.05 | 3.41 | 12.9 | 22.0 |
| time-series-ttl | pigeonhole | put at | 34244219 | 9.69 | 7.07 | 17.7 | 64.8 |
| time-series-ttl | pigeonhole | scan | 34243340 | 16.1 | 13.9 | 50.7 | 88.6 |
| time-series-ttl | rocksdb | get | 11914358 | 7.22 | 5.47 | 23.2 | 31.4 |
| time-series-ttl | rocksdb | put at | 23832003 | 10.0 | 9.66 | 19.7 | 65.3 |
| time-series-ttl | rocksdb | scan | 23828906 | 93.4 | 71.2 | 365 | 451 |
| time-series-ttl | sqlite-eav | get | 907717 | 7.48 | 6.50 | 26.5 | 45.6 |
| time-series-ttl | sqlite-eav | put at | 1818792 | 516 | 14.4 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1818501 | 8.73 | 7.68 | 28.5 | 47.6 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 5913011 | 17.3% | 1 → 6 | 1 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 5932767 | 17.3% | 1 → 8 | 0 | 2 | 6 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 5450292 | 15.9% | 1 → 6 | 2 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 6075280 | 17.7% | 2 → 5 | 1 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 4791692 | 14.0% | 1 → 3 | 1 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 3940570 | 11.5% | 1 → 0 | 0 | 0 | 4 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 359425 | 1.0% | 1 → 0 | 3 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 195133 | 0.6% | 1 → 0 | 1 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 210945 | 0.6% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 180764 | 0.5% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 212673 | 0.6% | 1 → 0 | 1 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 203125 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 181405 | 0.5% | 1 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 154549 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 241045 | 0.7% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 201543 | 0.6% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 31 | 2005.9 | 367 | 119 | 18 (41) | 0 | 0 | 0 | 0.0 | 0.01 | 0.02 | 0.00 | 9983 (759) | 0 |
