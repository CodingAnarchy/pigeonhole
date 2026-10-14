**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.66 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 73887365 | 16 | 1.10M | 8.77 | 51.2 | 537 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 59282333 | 16 | 376.7K | 13.2 | 326 | 393 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4814100 | 16 | 75.2K | 9.15 | 3146 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 5760 MiB | 0 | 11631941 | 3.13 | 12.5 | 19.6 | 3146800 | 3.20 | 12.8 | 19.8 |
| time-series-ttl | rocksdb | 3047 MiB | 0 | 9335745 | 5.05 | 22.8 | 30.3 | 2520433 | 5.31 | 22.3 | 28.7 |
| time-series-ttl | sqlite-eav | 606 MiB | 0 | 776627 | 6.50 | 26.2 | 45.1 | 185017 | 6.43 | 26.1 | 43.5 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 14778741 | 3.81 | 3.15 | 12.6 | 19.7 |
| time-series-ttl | pigeonhole | put at | 29556545 | 17.2 | 7.20 | 436 | 582 |
| time-series-ttl | pigeonhole | scan | 29552079 | 15.4 | 13.4 | 48.4 | 73.2 |
| time-series-ttl | rocksdb | get | 11856178 | 6.99 | 5.12 | 22.7 | 30.1 |
| time-series-ttl | rocksdb | put at | 23713965 | 10.1 | 9.73 | 19.7 | 77.8 |
| time-series-ttl | rocksdb | scan | 23712190 | 92.4 | 70.7 | 358 | 412 |
| time-series-ttl | sqlite-eav | get | 961644 | 7.43 | 6.50 | 26.1 | 44.8 |
| time-series-ttl | sqlite-eav | put at | 1926455 | 505 | 14.1 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1926001 | 8.57 | 7.55 | 28.0 | 47.4 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 4995356 | 16.9% | 1 → 8 | 0 | 2 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 5061194 | 17.1% | 2 → 9 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4910590 | 16.6% | 1 → 4 | 0 | 0 | 5 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 4481455 | 15.2% | 2 → 6 | 1 | 0 | 4 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 5108646 | 17.3% | 3 → 1 | 2 | 0 | 5 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 2513584 | 8.5% | 1 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 650992 | 2.2% | 1 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 203697 | 0.7% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 196879 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 148378 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 218372 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 149883 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 164311 | 0.6% | 2 → 0 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 226495 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 129670 | 0.4% | 2 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 397043 | 1.3% | 3 → 0 | 2 | 0 | 3 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 25 | 1360.2 | 330 | 102 | 23 (50) | 0 | 0 | 0 | 0.0 | 0.02 | 0.03 | 0.02 | 15459 (1595) | 0 |
