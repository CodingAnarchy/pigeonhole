**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.61 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 104730532 | 16 | 1.65M | 4.77 | 35.3 | 51.2 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 55963308 | 16 | 588.0K | 25.9 | 53.8 | 71.2 |
| ycsb-b | sqlite-eav | page_cache=320MiB buffered | 1000000 | 14139693 | 16 | 229.2K | 13.8 | 1098 | 8290 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 2688 MiB | 0 | 459095 | 15.0 | 36.6 | 76.3 | 99029801 | 4.22 | 35.6 | 51.2 |
| ycsb-b | rocksdb | 1335 MiB | 0 | 458432 | 15.3 | 29.8 | 39.2 | 52703939 | 26.9 | 54.3 | 71.7 |
| ycsb-b | sqlite-eav | 1788 MiB | 0 | 427018 | 13.7 | 95.7 | 153 | 13003965 | 13.1 | 93.2 | 142 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | put | 5241636 | 9.54 | 6.85 | 16.8 | 37.4 |
| ycsb-b | pigeonhole | row read | 99488896 | 8.92 | 4.29 | 35.6 | 51.2 |
| ycsb-b | rocksdb | put | 2800937 | 6.97 | 6.46 | 14.2 | 20.5 |
| ycsb-b | rocksdb | row read | 53162371 | 27.4 | 26.8 | 54.3 | 71.7 |
| ycsb-b | sqlite-eav | put | 708710 | 979 | 54.8 | 18350 | 53477 |
| ycsb-b | sqlite-eav | row read | 13430983 | 21.3 | 13.1 | 93.2 | 142 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2856598 | 54.5% | 3 → 6 | 0 | 0 | 17 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1115153 | 21.3% | 2 → 3 | 0 | 0 | 9 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1047987 | 20.0% | 2 → 3 | 0 | 0 | 10 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 105638 | 2.0% | 3 → 0 | 0 | 0 | 3 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 116260 | 2.2% | 2 → 0 | 0 | 0 | 2 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 16 | 5 | 109.7 | 78 | 27 | 5 (27) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 268 (5) | 0 |
