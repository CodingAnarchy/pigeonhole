**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.38 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 564754 | 16 | 6.2K | 1004 | 35652 | 95420 |
| transfer | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2574926 | 16 | 54.6K | 146 | 3228 | 6095 |
| transfer | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 916360 | 16 | 19.4K | 44.5 | 102 | 78643 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 24463 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 21185 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 76663 | 13.6% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 20352 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 55577 | 9.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 24439 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 51797 | 9.2% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 37668 | 6.7% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 23369 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 21196 | 3.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 42705 | 7.6% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 48515 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 24364 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 46650 | 8.3% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 19782 | 3.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 26029 | 4.6% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | 16 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 3.92 | 6.53 | 2.84 | 354370 (45926) | 0 |
