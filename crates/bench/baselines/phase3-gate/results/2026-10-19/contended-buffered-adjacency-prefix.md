**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.27 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 21377074 | 16 | 166.4K | 27.9 | 1360 | 3408 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7234862 | 16 | 54.9K | 57.9 | 2671 | 3080 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3275309 | 16 | 24.4K | 228 | 4948 | 6947 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 17102630 | 62.8 | 28.0 | 426 | 639 |
| adjacency-prefix | pigeonhole | put | 2137670 | 14.2 | 11.3 | 21.6 | 387 |
| adjacency-prefix | pigeonhole | scan | 2136774 | 445 | 124 | 3408 | 3949 |
| adjacency-prefix | rocksdb | filtered scan | 5787877 | 317 | 59.9 | 2703 | 3113 |
| adjacency-prefix | rocksdb | put | 723482 | 6.62 | 6.50 | 13.7 | 19.1 |
| adjacency-prefix | rocksdb | scan | 723503 | 368 | 131 | 2720 | 3047 |
| adjacency-prefix | sqlite-eav | filtered scan | 2620677 | 688 | 228 | 5079 | 6488 |
| adjacency-prefix | sqlite-eav | put | 327316 | 437 | 101 | 3539 | 18481 |
| adjacency-prefix | sqlite-eav | scan | 327316 | 614 | 297 | 4030 | 5407 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1944202 | 90.9% | 1 → 16 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 11681 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 29801 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 10165 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 6400 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 7494 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 15788 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 13457 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 19364 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 10285 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 12241 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 3028 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 12323 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 10689 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 8077 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 22675 | 1.1% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 7 | 323.3 | 31 | 16 | 3 (24) | 0 | 0 | 0 | 0.0 | 0.06 | 0.06 | 0.00 | 114 (0) | 0 |
