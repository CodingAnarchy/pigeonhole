**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.80 [non-reference (D5)]
Build: release, rev a9ecfc773395. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 1000000 | 1 | 174.2K | 4.83 | 14.1 | 22.0 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 1000000 | 1 | 101.3K | 6.14 | 28.8 | 36.6 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1600 MiB | 0 | 109752 | 9.09 | 17.8 | 26.6 | 390163 | 4.70 | 13.7 | 20.2 |
| ycsb-a | rocksdb | 1256 MiB | 0 | 109752 | 10.8 | 16.3 | 18.9 | 390163 | 18.4 | 31.7 | 39.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 500085 | 5.80 | 4.70 | 10.5 | 16.8 |
| ycsb-a | pigeonhole | row read | 499915 | 5.61 | 5.63 | 15.4 | 22.9 |
| ycsb-a | rocksdb | put | 500085 | 3.31 | 3.12 | 5.82 | 8.38 |
| ycsb-a | rocksdb | row read | 499915 | 16.3 | 14.1 | 30.8 | 38.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 322976 | 64.6% | 6 → 10 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 177109 | 35.4% | 10 → 6 | 0 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 1 | 1 | 55.6 | 5 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.07 | 0.07 | 0.00 | 0 (0) | 0 |
