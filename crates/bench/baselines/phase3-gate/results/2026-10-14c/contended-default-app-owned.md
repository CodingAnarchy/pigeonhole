**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.11 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 18000000 | 16 | 358.3K | 365 | 3998 | 26083 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 1215671 | 6.8% | 3 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 1123578 | 6.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 865372 | 4.8% | 3 → 4 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 988074 | 5.5% | 1 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 1158084 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 1204241 | 6.7% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1142546 | 6.3% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 1092096 | 6.1% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 1051924 | 5.8% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 1119132 | 6.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 1164524 | 6.5% | 3 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 1215585 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 1202048 | 6.7% | 2 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 1309126 | 7.3% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1091750 | 6.1% | 2 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 1056249 | 5.9% | 1 → 2 | 1 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 5 | 179.4 | 266 | 69 | 1390 (43) | 0 | 0 | 0 | 0.0 | 0.00 | 0.44 | 0.00 | 183148007 (20739940) | 0 |
