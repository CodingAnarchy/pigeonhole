**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 6.14 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 18000000 | 16 | 388.0K | 330 | 3981 | 18219 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 1203957 | 6.7% | 2 → 3 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 1326114 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 1265310 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 1095528 | 6.1% | 1 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 905562 | 5.0% | 1 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 1256013 | 7.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1006740 | 5.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 1270577 | 7.1% | 1 → 1 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 1070379 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 1040886 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 1040607 | 5.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 1109592 | 6.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 1097450 | 6.1% | 2 → 3 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 1206333 | 6.7% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1218911 | 6.8% | 2 → 2 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 886041 | 4.9% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 4 | 112.3 | 270 | 72 | 1218 (21) | 0 | 0 | 0 | 0.0 | 0.00 | 0.36 | 0.00 | 165384217 (19822884) | 0 |
