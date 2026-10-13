**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.99 [non-reference (D5)]
Build: release, rev 8654ad6924a9. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 173.5K | 4.58 | 14.1 | 422 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1600 MiB | 0 | 109752 | 8.70 | 17.4 | 25.2 | 390163 | 4.45 | 13.4 | 20.1 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 6.13 | 4.48 | 11.0 | 494 |
| ycsb-a | pigeonhole | row read | 499915 | 5.32 | 5.25 | 15.0 | 22.3 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 218957 | 43.8% | 3 → 3 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 281128 | 56.2% | 4 → 4 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 2 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.06 | 0.06 | 0.00 | 0 (0) | 0 |
