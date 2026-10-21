**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.49 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 79388436 | 16 | 1.19M | 8.83 | 49.7 | 85.5 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 59749505 | 16 | 376.1K | 13.2 | 328 | 395 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4832221 | 16 | 75.7K | 9.15 | 3146 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 6976 MiB | 0 | 12494847 | 3.38 | 13.0 | 22.7 | 3382426 | 3.46 | 13.4 | 23.6 |
| time-series-ttl | rocksdb | 3074 MiB | 0 | 9408467 | 4.83 | 22.7 | 30.1 | 2540336 | 5.12 | 22.1 | 28.3 |
| time-series-ttl | sqlite-eav | 606 MiB | 0 | 779556 | 6.46 | 25.6 | 43.8 | 185776 | 6.43 | 25.6 | 43.3 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 15877273 | 4.06 | 3.39 | 13.1 | 22.9 |
| time-series-ttl | pigeonhole | put at | 31757356 | 9.69 | 6.94 | 24.3 | 90.6 |
| time-series-ttl | pigeonhole | scan | 31753807 | 20.0 | 17.0 | 56.8 | 95.2 |
| time-series-ttl | rocksdb | get | 11948803 | 6.85 | 4.89 | 22.5 | 29.7 |
| time-series-ttl | rocksdb | put at | 23900677 | 10.1 | 9.66 | 19.8 | 79.9 |
| time-series-ttl | rocksdb | scan | 23900025 | 92.6 | 70.7 | 360 | 412 |
| time-series-ttl | sqlite-eav | get | 965332 | 7.38 | 6.46 | 25.6 | 43.8 |
| time-series-ttl | sqlite-eav | put at | 1933527 | 498 | 14.0 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1933362 | 8.51 | 7.55 | 27.5 | 46.6 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 4165650 | 13.1% | 1 → 9 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 6326606 | 19.9% | 1 → 8 | 0 | 5 | 8 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2691068 | 8.5% | 1 → 9 | 1 | 12 | 5 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2517838 | 7.9% | 2 → 2 | 1 | 9 | 8 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 3413388 | 10.7% | 1 → 1 | 2 | 0 | 5 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 4080090 | 12.8% | 1 → 5 | 17 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1378576 | 4.3% | 1 → 0 | 2 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1081128 | 3.4% | 2 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1232521 | 3.9% | 1 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 543527 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 894527 | 2.8% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 770723 | 2.4% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 464053 | 1.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 871721 | 2.7% | 1 → 0 | 16 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 593355 | 1.9% | 3 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 732585 | 2.3% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 25 | 1034.9 | 373 | 159 | 22 (78) | 0 | 0 | 1 | 0.1 | 0.00 | 0.01 | 0.00 | 10571 (1149) | 17 |
