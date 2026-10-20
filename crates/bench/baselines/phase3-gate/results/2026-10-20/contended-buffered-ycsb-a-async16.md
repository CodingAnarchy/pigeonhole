**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 7.99 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 1000000 | 21296724 | 16 | 473.3K | 144 | 11141 | 18088 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2368 MiB | 0 | 414588 | 135 | 1180 | 1786 | 10232625 | 174 | 1081 | 1655 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 10649511 | 568 | 119 | 13828 | 20054 |
| ycsb-a | pigeonhole | row read | 10647213 | 243 | 173 | 1090 | 1663 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 0 | 844566 | 7.9% | 3 → 4 | 1 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 1 | 1371936 | 12.9% | 1 → 2 | 1 | 0 | 8 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 2 | 852502 | 8.0% | 2 → 3 | 3 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 3 | 886669 | 8.3% | 1 → 2 | 1 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 4 | 766446 | 7.2% | 0 → 1 | 1 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 5 | 685421 | 6.4% | 0 → 1 | 2 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 6 | 563661 | 5.3% | 0 → 3 | 2 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 7 | 539118 | 5.1% | 0 → 2 | 1 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 8 | 666537 | 6.3% | 0 → 2 | 1 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 9 | 701318 | 6.6% | 0 → 2 | 1 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 10 | 666918 | 6.3% | 0 → 2 | 1 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 11 | 595405 | 5.6% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 12 | 300728 | 2.8% | 0 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 13 | 359398 | 3.4% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 14 | 447962 | 4.2% | 0 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on async=16 | 15 | 400926 | 3.8% | 0 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 6750 | 77309.6 | 2779 | 96 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.02 | 0.03 | 0.00 | 170533 (7614) | 0 |
