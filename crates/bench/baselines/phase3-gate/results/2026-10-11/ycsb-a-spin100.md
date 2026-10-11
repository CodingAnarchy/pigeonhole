**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.91 [non-reference (D5)]
Build: release, rev 8f675ed0c433. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 1000000 | 1000000 | 1 | 115.7K | 9.47 | 16.2 | 24.7 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1824 MiB | 0 | 109752 | 9.02 | 18.3 | 26.0 | 390163 | 4.70 | 13.6 | 19.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 11.0 | 10.6 | 16.9 | 29.2 |
| ycsb-a | pigeonhole | row read | 499915 | 6.16 | 5.28 | 15.5 | 22.4 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 0 | 10605 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 1 | 49898 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 2 | 8437 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 3 | 8122 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 4 | 27416 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 5 | 10682 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 6 | 31969 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 7 | 7101 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 8 | 694 | 0.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 9 | 33574 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 10 | 27533 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 11 | 4995 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 12 | 35238 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 13 | 4688 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 14 | 8150 | 1.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 15 | 6118 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 16 | 20499 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 17 | 3701 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 18 | 6524 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 19 | 9147 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 20 | 10378 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 21 | 4850 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 22 | 13369 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 23 | 10153 | 2.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 24 | 20000 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 25 | 22326 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 26 | 6607 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 27 | 2292 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 28 | 30721 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 29 | 25785 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 30 | 27169 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=100us | 31 | 11344 | 2.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 8 | 1.4 | 5 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.48 | 0.48 | 0.00 | 0 (0) | 0 |
