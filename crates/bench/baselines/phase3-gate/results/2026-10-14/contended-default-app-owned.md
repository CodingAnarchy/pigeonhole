**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.73 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 18000000 | 16 | 354.1K | 334 | 3998 | 28180 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 1106152 | 6.1% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 1222506 | 6.8% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 1170315 | 6.5% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 1268334 | 7.0% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 1048131 | 5.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 854199 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1010774 | 5.6% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 1194624 | 6.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 1147536 | 6.4% | 1 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 1053261 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 1274382 | 7.1% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 1142055 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 1275075 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 1170180 | 6.5% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1088406 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 974070 | 5.4% | 2 → 1 | 0 | 0 | 2 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 1 | 65.4 | 260 | 67 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.00 | 0.54 | 0.00 | 211440783 (23110426) | 0 |
