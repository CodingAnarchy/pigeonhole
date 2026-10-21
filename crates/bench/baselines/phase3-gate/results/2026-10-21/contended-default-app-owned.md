**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.94 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 6000000 | 16 | 119.6K | 123 | 48234 | 81789 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 156399 | 2.6% | 1 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 359988 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 400113 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 333798 | 5.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 411612 | 6.9% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 419508 | 7.0% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 342660 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 379764 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 381291 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 415245 | 6.9% | 4 → 4 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 499935 | 8.3% | 3 → 2 | 0 | 0 | 1 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 368676 | 6.1% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 442788 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 413436 | 6.9% | 3 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 433074 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 241713 | 4.0% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 0 | 0.0 | 127 | 49 | 1649 (180) | 0 | 0 | 1 | 9.5 | 0.00 | 3.34 | 0.00 | 7540608 (1065415) | 0 |
