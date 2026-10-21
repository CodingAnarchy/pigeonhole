**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.37 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 701740 | 16 | 5.6K | 995 | 41943 | 119538 |
| transfer | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2503161 | 16 | 54.0K | 148 | 3260 | 6160 |
| transfer | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 868512 | 16 | 18.9K | 44.8 | 102 | 53740 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 55284 | 7.9% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 31452 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 42822 | 6.1% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 59298 | 8.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 28950 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 44868 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 41509 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 38331 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 43381 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 43820 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 40536 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 71956 | 10.3% | 2 → 2 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 35433 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 66740 | 9.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 17399 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| transfer | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 39961 | 5.7% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| transfer | pigeonhole | 16 | 0 | 0.0 | 0 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 4.17 | 6.16 | 2.00 | 252334 (16261) | 0 |
