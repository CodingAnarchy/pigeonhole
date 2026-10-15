**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.13 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 21616709 | 16 | 168.5K | 28.5 | 1311 | 3047 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7201396 | 16 | 54.8K | 57.9 | 2687 | 2998 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3251482 | 16 | 24.2K | 237 | 4817 | 6816 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 17294105 | 63.1 | 28.7 | 414 | 664 |
| adjacency-prefix | pigeonhole | put | 2161656 | 15.0 | 11.4 | 22.5 | 729 |
| adjacency-prefix | pigeonhole | scan | 2160948 | 429 | 123 | 3047 | 3572 |
| adjacency-prefix | rocksdb | filtered scan | 5761003 | 317 | 59.9 | 2720 | 2998 |
| adjacency-prefix | rocksdb | put | 720337 | 6.69 | 6.53 | 14.0 | 19.6 |
| adjacency-prefix | rocksdb | scan | 720056 | 374 | 133 | 2769 | 3064 |
| adjacency-prefix | sqlite-eav | filtered scan | 2601680 | 694 | 237 | 4882 | 6259 |
| adjacency-prefix | sqlite-eav | put | 324806 | 441 | 104 | 3555 | 18612 |
| adjacency-prefix | sqlite-eav | scan | 324996 | 620 | 305 | 3752 | 5210 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1984200 | 91.8% | 1 → 16 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 12136 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 11476 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 3554 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 13266 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 6275 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 4292 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 25706 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 3989 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 273 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 16322 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 17917 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 2328 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 12988 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 10928 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 36006 | 1.7% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 7 | 315.9 | 31 | 17 | 3 (28) | 0 | 0 | 0 | 0.0 | 0.06 | 0.06 | 0.00 | 96 (1) | 0 |
