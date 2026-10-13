**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.79 [non-reference (D5)]
Build: release, rev a9ecfc773395. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 187.4K | 4.38 | 13.6 | 26.2 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1664 MiB | 0 | 109752 | 8.51 | 17.0 | 25.6 | 390163 | 4.86 | 13.1 | 19.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 5.25 | 4.19 | 10.1 | 506 |
| ycsb-a | pigeonhole | row read | 499915 | 5.35 | 5.63 | 14.8 | 22.0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 1 | 29.0 | 4 | 2 | 1 (3) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.00 | 0 (0) | 0 |
