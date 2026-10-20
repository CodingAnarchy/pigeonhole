**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.60 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 1000000 | 32288052 | 16 | 672.8K | 24.6 | 453 | 774 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2880 MiB | 0 | 437349 | 105 | 565 | 827 | 15703067 | 31.1 | 510 | 795 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 16147636 | 55.2 | 20.2 | 354 | 688 |
| ycsb-a | pigeonhole | row read | 16140416 | 82.6 | 31.7 | 512 | 795 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 0 | 3394172 | 21.0% | 2 → 8 | 0 | 0 | 11 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 1 | 2631944 | 16.3% | 1 → 7 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 2 | 2581328 | 16.0% | 2 → 6 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 3 | 2522225 | 15.6% | 1 → 2 | 0 | 0 | 8 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 4 | 1798532 | 11.1% | 2 → 5 | 0 | 0 | 10 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 5 | 1086589 | 6.7% | 2 → 0 | 0 | 0 | 11 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 6 | 658421 | 4.1% | 1 → 0 | 0 | 0 | 9 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 7 | 384741 | 2.4% | 1 → 0 | 0 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 8 | 88962 | 0.6% | 1 → 0 | 1 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 9 | 230168 | 1.4% | 3 → 0 | 0 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 10 | 136273 | 0.8% | 2 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 11 | 105597 | 0.7% | 2 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 12 | 130867 | 0.8% | 2 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 13 | 145989 | 0.9% | 2 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 14 | 120377 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=4 | 15 | 131451 | 0.8% | 2 → 0 | 0 | 0 | 2 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 67 | 1923.0 | 225 | 81 | 6 (15) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.00 | 176823 (20554) | 0 |
