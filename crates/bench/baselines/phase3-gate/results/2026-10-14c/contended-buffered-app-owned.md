**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.23 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 38000000 | 16 | 843.9K | 166 | 528 | 11928 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 2736342 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 2293699 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 2723726 | 7.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 2319425 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 2630930 | 6.9% | 1 → 5 | 4 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 2814622 | 7.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1593720 | 4.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 2768500 | 7.3% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 2878709 | 7.6% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 2345075 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 1627492 | 4.3% | 2 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 2109836 | 5.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 2591467 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 2435876 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1891336 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 2239245 | 5.9% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 16 | 1166.2 | 571 | 145 | 1860 (33) | 0 | 0 | 1 | 43.3 | 0.00 | 0.17 | 0.00 | 186966597 (39073256) | 0 |
