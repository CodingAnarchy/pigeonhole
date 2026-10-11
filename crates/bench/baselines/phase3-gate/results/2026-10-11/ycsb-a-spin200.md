**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.87 [non-reference (D5)]
Build: release, rev 8f675ed0c433. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 1000000 | 1000000 | 1 | 116.7K | 9.02 | 16.6 | 38.1 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1872 MiB | 0 | 109752 | 9.09 | 17.4 | 24.8 | 390163 | 4.74 | 12.9 | 17.0 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 10.8 | 11.1 | 17.7 | 44.5 |
| ycsb-a | pigeonhole | row read | 499915 | 6.22 | 5.38 | 15.1 | 21.1 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 0 | 15221 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 1 | 68163 | 13.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 2 | 22050 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 3 | 35080 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 4 | 7308 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 5 | 22441 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 6 | 32545 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 7 | 22966 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 8 | 9540 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 9 | 22472 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 10 | 5510 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 11 | 2839 | 0.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 12 | 6403 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 13 | 2682 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 14 | 11977 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 15 | 11317 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 16 | 5757 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 17 | 16877 | 3.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 18 | 25924 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 19 | 21908 | 4.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 20 | 9465 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 21 | 1771 | 0.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 22 | 24170 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 23 | 2734 | 0.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 24 | 6242 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 25 | 5321 | 1.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 26 | 14666 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 27 | 29102 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 28 | 7168 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 29 | 5830 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 30 | 9498 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=200us | 31 | 15138 | 3.0% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 50 | 7.2 | 2 | 1 | 0 (0) | 0 | 0 | 1 | 0.2 | 0.41 | 0.41 | 0.01 | 0 (0) | 0 |
