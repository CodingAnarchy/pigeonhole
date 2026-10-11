**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.35 [non-reference (D5)]
Build: release, rev 8f675ed0c433. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 1000000 | 1000000 | 1 | 117.5K | 6.72 | 22.8 | 44.3 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1728 MiB | 0 | 109752 | 9.66 | 19.7 | 27.0 | 390163 | 5.18 | 14.1 | 19.5 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 10.2 | 9.73 | 31.5 | 45.6 |
| ycsb-a | pigeonhole | row read | 499915 | 6.73 | 5.70 | 16.6 | 22.9 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 0 | 6039 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 1 | 83336 | 16.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 2 | 15732 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 3 | 48877 | 9.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 4 | 3592 | 0.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 5 | 11064 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 6 | 4884 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 7 | 8996 | 1.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 8 | 11910 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 9 | 4953 | 1.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 10 | 10453 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 11 | 15938 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 12 | 9643 | 1.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 13 | 39762 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 14 | 13601 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 15 | 15449 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 16 | 11296 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 17 | 15969 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 18 | 5899 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 19 | 5849 | 1.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 20 | 15792 | 3.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 21 | 7351 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 22 | 7556 | 1.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 23 | 6894 | 1.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 24 | 15062 | 3.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 25 | 14439 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 26 | 25740 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 27 | 29308 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 28 | 1107 | 0.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 29 | 6643 | 1.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 30 | 17397 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(32) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on shard_spin=500us | 31 | 9554 | 1.9% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.25 | 0.25 | 0.02 | 0 (0) | 0 |
