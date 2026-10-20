**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.14 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 21615018 | 16 | 168.0K | 28.8 | 1360 | 3015 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7272455 | 16 | 55.3K | 57.1 | 2687 | 3047 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3156637 | 16 | 25.3K | 226 | 4588 | 6619 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 17293038 | 62.7 | 28.8 | 412 | 659 |
| adjacency-prefix | pigeonhole | put | 2161308 | 11.2 | 7.20 | 21.4 | 494 |
| adjacency-prefix | pigeonhole | scan | 2160672 | 438 | 126 | 3015 | 3719 |
| adjacency-prefix | rocksdb | filtered scan | 5817952 | 314 | 59.1 | 2720 | 3031 |
| adjacency-prefix | rocksdb | put | 727331 | 6.71 | 6.59 | 13.8 | 18.9 |
| adjacency-prefix | rocksdb | scan | 727172 | 372 | 132 | 2785 | 3113 |
| adjacency-prefix | sqlite-eav | filtered scan | 2525664 | 657 | 226 | 4686 | 6029 |
| adjacency-prefix | sqlite-eav | put | 315500 | 451 | 101 | 3572 | 18612 |
| adjacency-prefix | sqlite-eav | scan | 315473 | 577 | 289 | 3555 | 4948 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1962563 | 90.8% | 1 → 16 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 8459 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 11578 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 16827 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 7605 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 17222 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 26363 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 13493 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 16795 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 5704 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 9113 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 16434 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 992 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 13773 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 8290 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 26097 | 1.2% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 7 | 284.2 | 33 | 18 | 3 (27) | 0 | 0 | 0 | 0.0 | 0.02 | 0.02 | 0.00 | 112 (0) | 0 |
