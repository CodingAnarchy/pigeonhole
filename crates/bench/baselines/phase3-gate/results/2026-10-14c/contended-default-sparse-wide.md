**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.12 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 10265028 | 16 | 68.7K | 45.6 | 2687 | 4162 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10287427 | 16 | 58.7K | 50.7 | 3555 | 5865 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2913553 | 16 | 34.0K | 30.1 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3840 MiB | 0 | 370786 | 7.10 | 56.6 | 124 | 5784529 | 12.4 | 2507 | 3949 |
| sparse-wide | rocksdb | 2433 MiB | 0 | 370974 | 16.6 | 42.2 | 54.0 | 5797638 | 17.3 | 3293 | 5538 |
| sparse-wide | sqlite-eav | 2815 MiB | 0 | 235672 | 9.60 | 47.4 | 81.4 | 1511901 | 11.6 | 1057 | 1663 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 4102928 | 9.86 | 7.20 | 36.9 | 80.9 |
| sparse-wide | pigeonhole | put | 2054653 | 106 | 61.2 | 1171 | 3015 |
| sparse-wide | pigeonhole | row read | 2052387 | 405 | 62.5 | 3228 | 4588 |
| sparse-wide | pigeonhole | scan | 2055060 | 597 | 332 | 3359 | 4784 |
| sparse-wide | rocksdb | get | 4111813 | 11.8 | 10.6 | 35.6 | 44.8 |
| sparse-wide | rocksdb | put | 2059242 | 223 | 140 | 2572 | 3949 |
| sparse-wide | rocksdb | row read | 2056799 | 526 | 58.6 | 4358 | 6750 |
| sparse-wide | rocksdb | scan | 2059573 | 587 | 111 | 4555 | 6947 |
| sparse-wide | sqlite-eav | get | 1165177 | 12.7 | 8.83 | 48.1 | 81.4 |
| sparse-wide | sqlite-eav | put | 582807 | 1916 | 82.9 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 582396 | 157 | 26.5 | 1303 | 1860 |
| sparse-wide | sqlite-eav | scan | 583173 | 202 | 58.4 | 1483 | 2097 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1026341 | 50.0% | 2 → 6 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 898340 | 43.7% | 2 → 6 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 39510 | 1.9% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 28962 | 1.4% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 27347 | 1.3% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 34153 | 1.7% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 15 | 510.2 | 56 | 18 | 2 (2) | 0 | 0 | 0 | 0.0 | 0.24 | 0.29 | 0.20 | 1295 (16) | 0 |
