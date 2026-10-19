**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.44 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 17542415 | 16 | 136.1K | 33.0 | 1286 | 2097 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7276988 | 16 | 55.2K | 57.1 | 2687 | 3047 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3251114 | 16 | 24.2K | 232 | 4915 | 7176 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 14036316 | 96.9 | 32.8 | 766 | 1507 |
| adjacency-prefix | pigeonhole | put | 1753689 | 24.4 | 18.4 | 43.8 | 1540 |
| adjacency-prefix | pigeonhole | scan | 1752410 | 375 | 130 | 2064 | 2572 |
| adjacency-prefix | rocksdb | filtered scan | 5821769 | 315 | 58.9 | 2720 | 3031 |
| adjacency-prefix | rocksdb | put | 727764 | 6.55 | 6.43 | 13.4 | 18.7 |
| adjacency-prefix | rocksdb | scan | 727455 | 371 | 131 | 2785 | 3097 |
| adjacency-prefix | sqlite-eav | filtered scan | 2601361 | 693 | 233 | 5014 | 6685 |
| adjacency-prefix | sqlite-eav | put | 324811 | 442 | 102 | 3539 | 18612 |
| adjacency-prefix | sqlite-eav | scan | 324942 | 619 | 301 | 3949 | 5603 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 45289 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 78310 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 141352 | 8.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 151056 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 65614 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 38014 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 111212 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 203314 | 11.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 61205 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 147666 | 8.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 69286 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 113381 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 27216 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 174689 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 85609 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 240476 | 13.7% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 0 | 0.0 | 10 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.00 | 342 (3) | 0 |
