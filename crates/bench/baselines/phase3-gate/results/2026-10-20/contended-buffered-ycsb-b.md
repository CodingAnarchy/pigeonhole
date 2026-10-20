**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.43 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 105199287 | 16 | 1.63M | 4.96 | 36.6 | 51.5 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 53340559 | 16 | 638.0K | 23.7 | 49.4 | 67.6 |
| ycsb-b | sqlite-eav | page_cache=320MiB buffered | 1000000 | 15732092 | 16 | 225.0K | 14.1 | 1106 | 8290 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 1984 MiB | 0 | 459096 | 15.4 | 36.6 | 58.1 | 99475105 | 4.42 | 36.9 | 51.5 |
| ycsb-b | rocksdb | 1349 MiB | 0 | 458271 | 14.1 | 28.2 | 37.1 | 50212015 | 24.7 | 49.9 | 67.6 |
| ycsb-b | sqlite-eav | 1788 MiB | 0 | 432369 | 13.7 | 93.2 | 144 | 14511252 | 13.4 | 94.7 | 143 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | put | 5265086 | 8.70 | 6.88 | 15.9 | 41.5 |
| ycsb-b | pigeonhole | row read | 99934201 | 9.23 | 4.45 | 36.9 | 51.5 |
| ycsb-b | rocksdb | put | 2670273 | 6.75 | 6.30 | 13.8 | 19.3 |
| ycsb-b | rocksdb | row read | 50670286 | 25.3 | 24.6 | 49.9 | 67.6 |
| ycsb-b | sqlite-eav | put | 788471 | 990 | 54.5 | 18350 | 53477 |
| ycsb-b | sqlite-eav | row read | 14943621 | 21.8 | 13.4 | 94.7 | 143 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2526569 | 48.0% | 3 → 16 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 2358633 | 44.8% | 7 → 0 | 0 | 0 | 11 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 182887 | 3.5% | 3 → 0 | 0 | 0 | 3 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 196997 | 3.7% | 3 → 0 | 0 | 0 | 3 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
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
| ycsb-b | pigeonhole | 16 | 8 | 247.1 | 65 | 26 | 8 (33) | 0 | 0 | 0 | 0.0 | 0.00 | 0.00 | 0.00 | 330 (1) | 0 |
