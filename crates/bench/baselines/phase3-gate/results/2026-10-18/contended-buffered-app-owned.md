**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.88 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 38000000 | 16 | 836.8K | 165 | 659 | 11600 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 2492548 | 6.6% | 2 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 2324364 | 6.1% | 2 → 6 | 5 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 2490501 | 6.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 2534790 | 6.7% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 2860968 | 7.5% | 3 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 2290312 | 6.0% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 2914334 | 7.7% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 2085231 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 2051715 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 2669887 | 7.0% | 1 → 2 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 2107746 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 1857069 | 4.9% | 1 → 2 | 1 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 2574709 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 1903258 | 5.0% | 1 → 3 | 1 | 0 | 2 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 2194253 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 2648315 | 7.0% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 587 | 1452.7 | 651 | 161 | 2639 (64) | 0 | 0 | 2 | 41.3 | 0.00 | 0.18 | 0.00 | 187899212 (37953382) | 0 |
