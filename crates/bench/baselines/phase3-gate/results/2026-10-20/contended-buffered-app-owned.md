**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 6.39 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 40000000 | 16 | 885.9K | 168 | 512 | 13894 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 3052686 | 7.6% | 2 → 2 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 2159200 | 5.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 2200380 | 5.5% | 1 → 2 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 3006120 | 7.5% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 2308210 | 5.8% | 1 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 2725180 | 6.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 2454580 | 6.1% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 2516114 | 6.3% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 2464060 | 6.2% | 1 → 2 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 2793800 | 7.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 2537080 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 2633880 | 6.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 2391720 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 2209850 | 5.5% | 2 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 2344760 | 5.9% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 2202380 | 5.5% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 1850 | 6098.2 | 701 | 176 | 2871 (94) | 0 | 0 | 2 | 3.0 | 0.00 | 0.16 | 0.00 | 198565165 (39744706) | 0 |
