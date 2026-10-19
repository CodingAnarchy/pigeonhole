**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.94 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 145.4K | 7.52 | 15.2 | 26.0 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2176 MiB | 0 | 109752 | 9.41 | 20.5 | 69.6 | 390163 | 4.96 | 15.4 | 27.8 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 7.71 | 8.45 | 12.7 | 16.2 |
| ycsb-a | pigeonhole | row read | 499915 | 5.96 | 6.05 | 17.0 | 39.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 119254 | 23.8% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 103377 | 20.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 122795 | 24.6% | 3 → 4 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=4 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 154659 | 30.9% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 6 | 4 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.30 | 0.30 | 0.00 | 0 (0) | 0 |
