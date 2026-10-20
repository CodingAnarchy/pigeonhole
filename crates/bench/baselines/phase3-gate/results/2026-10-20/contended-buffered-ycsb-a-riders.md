**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 10.34 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 1000000 | 60093670 | 16 | 925.8K | 9.09 | 68.1 | 113 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 4480 MiB | 0 | 454886 | 20.1 | 57.1 | 140 | 29588985 | 23.2 | 76.3 | 128 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 30049799 | 8.96 | 7.46 | 18.7 | 64.5 |
| ycsb-a | pigeonhole | row read | 30043871 | 24.3 | 23.0 | 76.3 | 129 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 0 | 4532118 | 15.1% | 1 → 4 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 1 | 4655254 | 15.5% | 2 → 3 | 1 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 2 | 4738055 | 15.8% | 2 → 4 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 3 | 4869485 | 16.2% | 2 → 3 | 0 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 4 | 4894045 | 16.3% | 2 → 3 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 5 | 2981396 | 9.9% | 3 → 0 | 0 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 6 | 960680 | 3.2% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 7 | 1189397 | 4.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 8 | 1229369 | 4.1% | 1 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on riders | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 2 | 25.2 | 323 | 88 | 8 (10) | 0 | 0 | 1 | 1.9 | 0.00 | 0.01 | 0.00 | 2147 (83) | 0 |
