**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 8.35 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 2436873 | 16 | 37.3K | 90.6 | 1352 | 1622 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7479942 | 16 | 105.9K | 130 | 379 | 2572 |
| ycsb-e | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2804091 | 16 | 42.7K | 177 | 3424 | 34079 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | put | 121634 | 102 | 60.2 | 185 | 5603 |
| ycsb-e | pigeonhole | scan | 2315239 | 444 | 96.3 | 1352 | 1622 |
| ycsb-e | rocksdb | put | 373813 | 350 | 133 | 3195 | 16122 |
| ycsb-e | rocksdb | scan | 7106129 | 140 | 129 | 350 | 406 |
| ycsb-e | sqlite-eav | put | 139833 | 3638 | 713 | 54002 | 129499 |
| ycsb-e | sqlite-eav | scan | 2664258 | 195 | 169 | 598 | 791 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 38645 | 31.8% | 2 → 3 | 1 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 131 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 24046 | 19.8% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 28567 | 23.5% | 1 → 2 | 1 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 30245 | 24.9% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | 16 | 0 | 0.0 | 14 | 5 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.05 | 1 (0) | 0 |
