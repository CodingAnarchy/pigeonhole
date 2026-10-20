**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.59 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 18737669 | 16 | 312.0K | 42.8 | 164 | 225 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 11451227 | 16 | 171.4K | 75.8 | 239 | 365 |
| ycsb-f | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2373079 | 16 | 38.5K | 30.8 | 80.4 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2176 MiB | 0 | 405673 | 17.2 | 34.8 | 79.9 | 8961382 | 14.9 | 41.5 | 59.1 |
| ycsb-f | rocksdb | 1403 MiB | 0 | 363995 | 19.1 | 38.1 | 47.1 | 5358524 | 29.3 | 71.2 | 89.1 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 195245 | 8.96 | 20.4 | 48.9 | 991514 | 7.87 | 18.0 | 44.8 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 9370614 | 87.3 | 78.8 | 182 | 270 |
| ycsb-f | pigeonhole | row read | 9367055 | 14.8 | 15.1 | 41.2 | 59.9 |
| ycsb-f | rocksdb | read-modify-write | 5728708 | 155 | 158 | 281 | 399 |
| ycsb-f | rocksdb | row read | 5722519 | 30.7 | 28.4 | 70.7 | 88.6 |
| ycsb-f | sqlite-eav | read-modify-write | 1186320 | 772 | 39.9 | 185 | 128975 |
| ycsb-f | sqlite-eav | row read | 1186759 | 8.03 | 8.10 | 18.4 | 45.3 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 625569 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 258865 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 712498 | 7.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 576812 | 6.2% | 1 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 828501 | 8.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 747274 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 803841 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 341794 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 699964 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 662237 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 790263 | 8.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 843495 | 9.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 430206 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 462888 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 221165 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 365242 | 3.9% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 0 | 0.0 | 99 | 26 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.12 | 0.44 | 0.50 | 33946 (4102) | 0 |
