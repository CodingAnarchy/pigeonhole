**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.92 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 16000000 | 16 | 321.6K | 46.8 | 20578 | 39059 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 890768 | 5.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 1159040 | 7.2% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 1065006 | 6.7% | 2 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 1083953 | 6.8% | 1 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 1174944 | 7.3% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 899281 | 5.6% | 3 → 3 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1020152 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 965729 | 6.0% | 3 → 4 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 860612 | 5.4% | 2 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 1057944 | 6.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 1016968 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 956400 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 961242 | 6.0% | 3 → 1 | 0 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 958017 | 6.0% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1123768 | 7.0% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 806176 | 5.0% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 3 | 73.4 | 241 | 94 | 2331 (262) | 0 | 0 | 2 | 57.4 | 0.00 | 1.53 | 0.00 | 30740053 (7973779) | 0 |
