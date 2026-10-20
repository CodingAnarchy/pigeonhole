**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.25 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 9006620 | 16 | 74.2K | 41.7 | 2441 | 3686 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10401535 | 16 | 59.5K | 49.9 | 3473 | 5669 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2852572 | 16 | 34.1K | 30.7 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3840 MiB | 0 | 358295 | 6.69 | 48.9 | 107 | 5042857 | 11.4 | 2294 | 3506 |
| sparse-wide | rocksdb | 2454 MiB | 0 | 371969 | 16.2 | 41.5 | 53.0 | 5865085 | 16.9 | 3211 | 5341 |
| sparse-wide | sqlite-eav | 2813 MiB | 0 | 233376 | 9.66 | 49.7 | 80.4 | 1477504 | 11.6 | 1057 | 1663 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3599975 | 8.91 | 6.69 | 31.0 | 61.2 |
| sparse-wide | pigeonhole | put | 1802747 | 88.2 | 58.1 | 668 | 2638 |
| sparse-wide | pigeonhole | row read | 1801177 | 365 | 54.5 | 2867 | 3949 |
| sparse-wide | pigeonhole | scan | 1802721 | 559 | 315 | 3015 | 4112 |
| sparse-wide | rocksdb | get | 4157238 | 11.5 | 10.4 | 34.6 | 43.5 |
| sparse-wide | rocksdb | put | 2082039 | 218 | 139 | 2523 | 3867 |
| sparse-wide | rocksdb | row read | 2079816 | 519 | 57.6 | 4260 | 6193 |
| sparse-wide | rocksdb | scan | 2082442 | 583 | 110 | 4456 | 6455 |
| sparse-wide | sqlite-eav | get | 1140640 | 12.8 | 8.83 | 50.2 | 81.9 |
| sparse-wide | sqlite-eav | put | 570623 | 1917 | 82.9 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 570240 | 157 | 26.6 | 1303 | 1860 |
| sparse-wide | sqlite-eav | scan | 571069 | 201 | 58.9 | 1475 | 2064 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 893784 | 49.6% | 1 → 6 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 781145 | 43.3% | 1 → 7 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 26619 | 1.5% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 17268 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 19116 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 16413 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 12562 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 11914 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 12789 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 11137 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 12 | 402.6 | 46 | 15 | 1 (3) | 0 | 0 | 0 | 0.0 | 0.13 | 0.24 | 0.20 | 667 (9) | 0 |
