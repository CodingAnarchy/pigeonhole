**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.23 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 11648795 | 16 | 66.2K | 46.1 | 2998 | 4817 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10379166 | 16 | 59.3K | 49.9 | 3490 | 5898 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2859938 | 16 | 33.8K | 30.2 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3968 MiB | 0 | 382086 | 7.36 | 53.8 | 114 | 6603674 | 12.6 | 2802 | 4555 |
| sparse-wide | rocksdb | 2443 MiB | 0 | 371760 | 16.3 | 41.2 | 52.2 | 5851950 | 16.9 | 3244 | 5538 |
| sparse-wide | sqlite-eav | 2813 MiB | 0 | 233668 | 9.66 | 48.6 | 80.9 | 1481686 | 11.6 | 1057 | 1647 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 4655789 | 10.1 | 7.46 | 37.4 | 83.5 |
| sparse-wide | pigeonhole | put | 2330857 | 127 | 61.7 | 1466 | 3604 |
| sparse-wide | pigeonhole | row read | 2329971 | 441 | 64.5 | 3604 | 5243 |
| sparse-wide | pigeonhole | scan | 2332178 | 616 | 319 | 3752 | 5472 |
| sparse-wide | rocksdb | get | 4148553 | 11.5 | 10.4 | 34.6 | 43.3 |
| sparse-wide | rocksdb | put | 2077416 | 219 | 139 | 2556 | 3916 |
| sparse-wide | rocksdb | row read | 2075157 | 523 | 57.6 | 4325 | 6554 |
| sparse-wide | rocksdb | scan | 2078040 | 583 | 109 | 4489 | 6783 |
| sparse-wide | sqlite-eav | get | 1143644 | 12.9 | 8.89 | 49.4 | 82.4 |
| sparse-wide | sqlite-eav | put | 572086 | 1923 | 83.5 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 571710 | 156 | 26.8 | 1303 | 1835 |
| sparse-wide | sqlite-eav | scan | 572498 | 197 | 58.1 | 1458 | 1999 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 522422 | 22.4% | 10 → 9 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 677721 | 29.1% | 8 → 7 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1096612 | 47.0% | 2 → 7 | 2 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 23292 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 7619 | 0.3% | 0 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 3191 | 0.1% | 0 → 0 | 0 | 0 | 1 |
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
| sparse-wide | pigeonhole | 16 | 5 | 152.7 | 64 | 20 | 5 (17) | 0 | 0 | 0 | 0.0 | 0.27 | 0.31 | 0.20 | 1994 (28) | 0 |
