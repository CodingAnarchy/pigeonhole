**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.86 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 22230857 | 16 | 172.9K | 28.2 | 1335 | 2785 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 7254860 | 16 | 55.1K | 57.9 | 2687 | 3064 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3293601 | 16 | 24.5K | 229 | 4882 | 7274 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 17785420 | 60.7 | 28.0 | 377 | 598 |
| adjacency-prefix | pigeonhole | put | 2223085 | 17.1 | 11.3 | 34.6 | 717 |
| adjacency-prefix | pigeonhole | scan | 2222352 | 422 | 124 | 2785 | 3310 |
| adjacency-prefix | rocksdb | filtered scan | 5803915 | 315 | 59.9 | 2720 | 3064 |
| adjacency-prefix | rocksdb | put | 725629 | 6.77 | 6.66 | 14.0 | 19.3 |
| adjacency-prefix | rocksdb | scan | 725316 | 373 | 133 | 2769 | 3113 |
| adjacency-prefix | sqlite-eav | filtered scan | 2635324 | 683 | 229 | 4981 | 6652 |
| adjacency-prefix | sqlite-eav | put | 329144 | 443 | 104 | 3555 | 18612 |
| adjacency-prefix | sqlite-eav | scan | 329133 | 614 | 295 | 3965 | 5636 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2039669 | 91.7% | 1 → 16 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 22484 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 9689 | 0.4% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 3502 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 7278 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 40097 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 13764 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 3348 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 16133 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 4559 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 4421 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 5699 | 0.3% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 17183 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 4805 | 0.2% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 3094 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 27360 | 1.2% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 6 | 361.4 | 33 | 18 | 3 (30) | 0 | 0 | 0 | 0.0 | 0.06 | 0.06 | 0.01 | 563 (13) | 0 |
