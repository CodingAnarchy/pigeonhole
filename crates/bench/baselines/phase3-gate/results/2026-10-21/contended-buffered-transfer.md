**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.68 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 310956 | 16 | 5.1K | 1065 | 48759 | 120062 |
| transfer | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 23181389 | 16 | 510.7K | 13.1 | 322 | 582 |
| transfer | sqlite-eav | page_cache=320MiB buffered | 1000000 | 1948458 | 16 | 42.0K | 16.3 | 313 | 78643 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 6536 | 2.1% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 33470 | 10.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 49072 | 15.8% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 19657 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 27391 | 8.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 7139 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 25741 | 8.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 16510 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 14911 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 8926 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 24649 | 7.9% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 19861 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 12301 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 28851 | 9.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 15941 | 5.1% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | 16 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 2.89 | 2.91 | 2.14 | 102987 (6677) | 0 |
