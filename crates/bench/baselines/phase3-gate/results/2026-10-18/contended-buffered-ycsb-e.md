**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 7.35 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 2003009 | 16 | 33.0K | 89.6 | 1606 | 2376 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 8841253 | 16 | 134.4K | 112 | 264 | 340 |
| ycsb-e | sqlite-eav | page_cache=320MiB buffered | 1000000 | 3010906 | 16 | 44.8K | 246 | 1630 | 18612 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | put | 100010 | 39.9 | 34.3 | 184 | 418 |
| ycsb-e | pigeonhole | scan | 1902999 | 507 | 99.3 | 1614 | 2392 |
| ycsb-e | rocksdb | put | 441263 | 13.5 | 13.1 | 24.1 | 36.9 |
| ycsb-e | rocksdb | scan | 8399990 | 123 | 117 | 264 | 342 |
| ycsb-e | sqlite-eav | put | 150126 | 2020 | 582 | 33554 | 79692 |
| ycsb-e | sqlite-eav | scan | 2860780 | 264 | 239 | 729 | 942 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 5274 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 105 | 0.1% | 1 → 0 | 0 | 0 | 1 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 927 | 0.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 25154 | 25.2% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 24231 | 24.2% | 2 → 2 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 13630 | 13.6% | 0 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 7195 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 9994 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 3699 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 6017 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 3784 | 3.8% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | 16 | 0 | 0.0 | 6 | 2 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.05 | 0.00 | 30 (0) | 0 |
