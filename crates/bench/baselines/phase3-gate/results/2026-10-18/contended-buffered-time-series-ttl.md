**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.95 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 82770367 | 16 | 1.19M | 10.2 | 47.4 | 101 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 59727896 | 16 | 378.1K | 13.2 | 326 | 406 |
| time-series-ttl | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4925699 | 16 | 75.3K | 9.21 | 3146 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 6656 MiB | 0 | 13026345 | 3.39 | 12.2 | 21.1 | 3528415 | 3.46 | 12.7 | 22.4 |
| time-series-ttl | rocksdb | 3074 MiB | 0 | 9405374 | 5.44 | 22.7 | 30.5 | 2539260 | 5.66 | 22.1 | 28.8 |
| time-series-ttl | sqlite-eav | 615 MiB | 0 | 794436 | 6.50 | 25.7 | 44.0 | 189667 | 6.46 | 25.9 | 44.0 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 16554760 | 3.90 | 3.39 | 12.4 | 21.4 |
| time-series-ttl | pigeonhole | put at | 33107999 | 11.4 | 8.32 | 38.4 | 130 |
| time-series-ttl | pigeonhole | scan | 33107608 | 19.2 | 16.6 | 52.5 | 85.0 |
| time-series-ttl | rocksdb | get | 11944634 | 7.13 | 5.47 | 22.5 | 30.1 |
| time-series-ttl | rocksdb | put at | 23892419 | 10.1 | 9.73 | 20.0 | 85.0 |
| time-series-ttl | rocksdb | scan | 23890843 | 91.9 | 70.1 | 362 | 432 |
| time-series-ttl | sqlite-eav | get | 984103 | 7.45 | 6.50 | 25.9 | 44.0 |
| time-series-ttl | sqlite-eav | put at | 1970887 | 500 | 14.1 | 8225 | 78643 |
| time-series-ttl | sqlite-eav | scan | 1970709 | 8.62 | 7.62 | 27.8 | 46.1 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2488489 | 7.5% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 3464341 | 10.5% | 1 → 1 | 35 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2350279 | 7.1% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1772390 | 5.4% | 2 → 3 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1825680 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1872277 | 5.7% | 1 → 1 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2546734 | 7.7% | 1 → 2 | 8 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1789823 | 5.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1841489 | 5.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1800934 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 1987536 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 2466123 | 7.4% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1805195 | 5.5% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1563460 | 4.7% | 1 → 12 | 0 | 4 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 1290476 | 3.9% | 2 → 16 | 0 | 13 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 2242773 | 6.8% | 1 → 2 | 1 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 1 | 9.3 | 358 | 129 | 4 (30) | 0 | 0 | 0 | 0.0 | 0.11 | 0.11 | 0.00 | 12749 (1075) | 25 |
