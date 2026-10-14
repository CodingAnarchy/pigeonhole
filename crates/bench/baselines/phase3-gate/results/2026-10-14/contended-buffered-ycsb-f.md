**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 5.37 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 31928143 | 16 | 432.9K | 23.9 | 158 | 1040 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 47762271 | 16 | 624.9K | 22.1 | 73.7 | 136 |
| ycsb-f | sqlite-eav | page_cache=320MiB buffered | 1000000 | 5040317 | 16 | 81.9K | 11.1 | 1106 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2368 MiB | 0 | 436679 | 14.5 | 31.4 | 45.3 | 15525636 | 13.8 | 41.7 | 56.3 |
| ycsb-f | rocksdb | 1459 MiB | 0 | 450440 | 20.1 | 42.0 | 51.7 | 23425778 | 32.8 | 77.8 | 102 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 277051 | 9.09 | 30.5 | 53.5 | 2241294 | 8.32 | 29.4 | 52.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 15965828 | 57.9 | 40.4 | 541 | 1335 |
| ycsb-f | pigeonhole | row read | 15962315 | 14.1 | 13.9 | 41.5 | 56.3 |
| ycsb-f | rocksdb | read-modify-write | 23886053 | 16.6 | 13.5 | 45.6 | 182 |
| ycsb-f | rocksdb | row read | 23876218 | 34.0 | 32.5 | 77.3 | 102 |
| ycsb-f | sqlite-eav | read-modify-write | 2521972 | 370 | 13.4 | 3211 | 78643 |
| ycsb-f | sqlite-eav | row read | 2518345 | 9.04 | 8.38 | 29.6 | 53.0 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 4237896 | 26.5% | 11 → 5 | 1 | 0 | 6 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 3923808 | 24.6% | 7 → 3 | 0 | 0 | 5 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4143081 | 25.9% | 0 → 2 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2541416 | 15.9% | 0 → 3 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 368575 | 2.3% | 0 → 1 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 310519 | 1.9% | 0 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 440533 | 2.8% | 0 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 22 | 1009.0 | 165 | 55 | 11 (25) | 0 | 0 | 0 | 0.0 | 0.03 | 0.04 | 0.50 | 30016 (856) | 0 |
