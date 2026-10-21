**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.57 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 22063864 | 16 | 170.8K | 28.3 | 1360 | 2867 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7295267 | 16 | 55.2K | 57.6 | 2671 | 3031 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3290117 | 16 | 24.5K | 230 | 4850 | 7078 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 17651742 | 60.9 | 28.3 | 365 | 569 |
| adjacency-prefix | pigeonhole | put | 2206649 | 12.0 | 7.04 | 22.1 | 590 |
| adjacency-prefix | pigeonhole | scan | 2205473 | 427 | 127 | 2867 | 3604 |
| adjacency-prefix | rocksdb | filtered scan | 5836197 | 315 | 59.6 | 2703 | 3031 |
| adjacency-prefix | rocksdb | put | 729698 | 6.71 | 6.56 | 14.0 | 19.5 |
| adjacency-prefix | rocksdb | scan | 729372 | 371 | 132 | 2753 | 3097 |
| adjacency-prefix | sqlite-eav | filtered scan | 2632593 | 684 | 230 | 4948 | 6554 |
| adjacency-prefix | sqlite-eav | put | 328759 | 444 | 102 | 3555 | 18612 |
| adjacency-prefix | sqlite-eav | scan | 328765 | 610 | 295 | 3883 | 5538 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1971940 | 89.4% | 1 → 16 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 18448 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 6289 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 8670 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2475 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 14114 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 9178 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 9111 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 28082 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 29932 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 20625 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 10966 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 7784 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 16194 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 12147 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 40694 | 1.8% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 7 | 327.7 | 35 | 17 | 3 (20) | 0 | 0 | 0 | 0.0 | 0.02 | 0.02 | 0.00 | 82 (0) | 0 |
