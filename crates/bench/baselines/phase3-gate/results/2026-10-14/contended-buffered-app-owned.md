**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.41 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 34000000 | 16 | 741.0K | 162 | 610 | 23855 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 2202265 | 6.5% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 2446453 | 7.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 2426699 | 7.1% | 1 → 5 | 4 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 2069836 | 6.1% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 2293725 | 6.7% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 2319378 | 6.8% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1901552 | 5.6% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 1483488 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 1580948 | 4.6% | 2 → 1 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 2464422 | 7.2% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 2506803 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 2080664 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 2167313 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 2457197 | 7.2% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 2089436 | 6.1% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 1509821 | 4.4% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 15 | 1048.1 | 516 | 136 | 2153 (42) | 0 | 0 | 1 | 1.1 | 0.00 | 0.31 | 0.00 | 223158022 (37772604) | 0 |
