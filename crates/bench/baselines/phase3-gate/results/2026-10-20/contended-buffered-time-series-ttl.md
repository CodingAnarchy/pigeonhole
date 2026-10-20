**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.62 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 82428410 | 16 | 1.21M | 9.66 | 48.9 | 86.0 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 60108083 | 16 | 381.3K | 13.2 | 317 | 383 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4705849 | 16 | 74.2K | 9.28 | 3146 | 33554 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 6272 MiB | 0 | 12972210 | 3.57 | 13.5 | 23.7 | 3513873 | 3.65 | 13.9 | 25.2 |
| time-series-ttl | rocksdb | 3095 MiB | 0 | 9465136 | 4.83 | 21.8 | 28.7 | 2555839 | 5.15 | 21.4 | 27.3 |
| time-series-ttl | sqlite-eav | 599 MiB | 0 | 759422 | 6.62 | 27.4 | 47.4 | 180507 | 6.59 | 27.5 | 47.9 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 16486083 | 4.29 | 3.57 | 13.6 | 23.9 |
| time-series-ttl | pigeonhole | put at | 32971134 | 10.4 | 7.55 | 27.0 | 86.0 |
| time-series-ttl | pigeonhole | scan | 32971193 | 19.1 | 16.3 | 58.1 | 98.3 |
| time-series-ttl | rocksdb | get | 12020975 | 6.75 | 4.93 | 21.8 | 28.4 |
| time-series-ttl | rocksdb | put at | 24044540 | 10.2 | 9.79 | 19.8 | 97.8 |
| time-series-ttl | rocksdb | scan | 24042568 | 91.0 | 70.7 | 350 | 399 |
| time-series-ttl | sqlite-eav | get | 939929 | 7.59 | 6.62 | 27.4 | 47.4 |
| time-series-ttl | sqlite-eav | put at | 1883144 | 509 | 14.3 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1882776 | 8.73 | 7.71 | 29.4 | 50.2 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 3367631 | 10.2% | 1 → 2 | 1 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 569482 | 1.7% | 2 → 16 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2245884 | 6.8% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2638651 | 8.0% | 2 → 5 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2562906 | 7.8% | 2 → 4 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 3625297 | 11.0% | 1 → 4 | 2 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 10075002 | 30.6% | 2 → 9 | 2 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 2610517 | 7.9% | 1 → 2 | 16 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 721995 | 2.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 562481 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 565425 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 735795 | 2.2% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 757112 | 2.3% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 508413 | 1.5% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 539486 | 1.6% | 2 → 0 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 885057 | 2.7% | 2 → 0 | 0 | 0 | 2 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 43 | 2266.8 | 351 | 132 | 15 (49) | 0 | 0 | 1 | 0.5 | 0.00 | 0.01 | 0.00 | 12020 (1294) | 46 |
