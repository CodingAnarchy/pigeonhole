**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 6.34 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1000000 | 18000000 | 16 | 367.6K | 332 | 4178 | 20578 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 0 | 1223154 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 1 | 1257255 | 7.0% | 1 → 3 | 2 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 2 | 1300050 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 3 | 825003 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 4 | 1016433 | 5.6% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 5 | 1020429 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 6 | 1031463 | 5.7% | 2 → 3 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 7 | 1073151 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 8 | 1329786 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 9 | 1156410 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 10 | 949419 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 11 | 1168839 | 6.5% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 12 | 1094382 | 6.1% | 2 → 2 | 1 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 13 | 1241550 | 6.9% | 2 → 2 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 14 | 1090413 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| skewed-multi-shard | pigeonhole | shards=16 memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on inline in-flight=16 pinned cross-batches=20% | 15 | 1222263 | 6.8% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| skewed-multi-shard | pigeonhole | 16 | 0 | 0.0 | 262 | 66 | 758 (15) | 0 | 0 | 1 | 46.1 | 0.00 | 0.35 | 0.00 | 170116712 (20204184) | 0 |
