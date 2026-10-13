**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.13 [non-reference (D5)]
Build: release, rev a9ecfc773395. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 156.5K | 4.64 | 15.0 | 496 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1536 MiB | 0 | 109752 | 9.02 | 18.4 | 26.0 | 390163 | 4.64 | 14.1 | 21.1 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 7.03 | 4.48 | 11.9 | 512 |
| ycsb-a | pigeonhole | row read | 499915 | 5.67 | 5.66 | 15.8 | 23.4 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 252376 | 50.5% | 3 → 3 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 247709 | 49.5% | 3 → 3 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 5 | 3 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.07 | 0.07 | 0.00 | 0 (0) | 0 |
