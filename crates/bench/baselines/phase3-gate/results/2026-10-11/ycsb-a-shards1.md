**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.74 [non-reference (D5)]
Build: release, rev 8f675ed0c433. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=1 memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 168.6K | 4.19 | 13.6 | 487 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1600 MiB | 0 | 109752 | 8.45 | 16.5 | 24.2 | 390163 | 4.77 | 13.2 | 18.6 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 5.67 | 3.95 | 8.51 | 510 |
| ycsb-a | pigeonhole | row read | 499915 | 6.11 | 5.25 | 14.5 | 20.9 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 199 | 94.9 | 5 | 2 | 1 (3) | 0 | 0 | 1 | 46.2 | 0.00 | 0.00 | 0.00 | 0 (0) | 0 |
