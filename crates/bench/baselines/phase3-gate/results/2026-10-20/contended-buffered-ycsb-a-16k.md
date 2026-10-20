**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 7.66 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 50000 | 5109607 | 16 | 113.3K | 41.7 | 741 | 18088 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 50000 | 3800633 | 16 | 36.6K | 211 | 2064 | 15729 |
| ycsb-a | sqlite-eav | page_cache=320MiB buffered | 50000 | 534017 | 16 | 11.0K | 131 | 33423 | 128975 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 51968 MiB | 0 | 35821 | 183 | 2671 | 7209 | 2517864 | 87.6 | 840 | 4555 |
| ycsb-a | rocksdb | 11368 MiB | 0 | 35728 | 209 | 475 | 659 | 1864019 | 485 | 2130 | 2736 |
| ycsb-a | sqlite-eav | 8107 MiB | 0 | 26769 | 140 | 582 | 741 | 240439 | 136 | 537 | 696 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 2555922 | 130 | 21.9 | 483 | 30671 |
| ycsb-a | pigeonhole | row read | 2553685 | 150 | 88.6 | 872 | 4686 |
| ycsb-a | rocksdb | put | 1900886 | 185 | 64.5 | 762 | 16253 |
| ycsb-a | rocksdb | row read | 1899747 | 676 | 479 | 2130 | 2736 |
| ycsb-a | sqlite-eav | put | 266809 | 2655 | 99.3 | 53740 | 179306 |
| ycsb-a | sqlite-eav | row read | 267208 | 151 | 137 | 541 | 700 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 193241 | 7.6% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 274313 | 10.7% | 1 → 1 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 619723 | 24.2% | 0 → 8 | 0 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 105477 | 4.1% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 150593 | 5.9% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 130080 | 5.1% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 61518 | 2.4% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 129270 | 5.1% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 133072 | 5.2% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 111091 | 4.3% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 204221 | 8.0% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 31220 | 1.2% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 57741 | 2.3% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 235938 | 9.2% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 64775 | 2.5% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 53649 | 2.1% | 0 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 299 | 14264.5 | 2677 | 858 | 57 (219) | 0 | 1 | 4 | 105.3 | 0.13 | 0.14 | 0.03 | 26029 (579) | 0 |
