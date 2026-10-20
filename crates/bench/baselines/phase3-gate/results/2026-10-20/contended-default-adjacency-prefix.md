**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.82 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 14789393 | 16 | 113.0K | 49.9 | 1614 | 2900 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 6575059 | 16 | 49.5K | 84.5 | 2671 | 3883 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2812238 | 16 | 25.4K | 185 | 4555 | 18743 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 11832957 | 106 | 33.5 | 938 | 2089 |
| adjacency-prefix | pigeonhole | put | 1478646 | 176 | 66.6 | 2540 | 3916 |
| adjacency-prefix | pigeonhole | scan | 1477790 | 390 | 128 | 2376 | 3195 |
| adjacency-prefix | rocksdb | filtered scan | 5259729 | 324 | 64.0 | 2638 | 3768 |
| adjacency-prefix | rocksdb | put | 657792 | 247 | 103 | 3113 | 4817 |
| adjacency-prefix | rocksdb | scan | 657538 | 389 | 143 | 2720 | 3899 |
| adjacency-prefix | sqlite-eav | filtered scan | 2249870 | 525 | 175 | 4112 | 5702 |
| adjacency-prefix | sqlite-eav | put | 281275 | 1547 | 244 | 18743 | 79167 |
| adjacency-prefix | sqlite-eav | scan | 281093 | 451 | 215 | 3146 | 4784 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 61295 | 4.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 103682 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 126940 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 42834 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 71367 | 4.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 74641 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 186982 | 12.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 39623 | 2.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 41918 | 2.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 95469 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 93125 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 72955 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 25296 | 1.7% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 32898 | 2.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 107444 | 7.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 302177 | 20.4% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 0 | 0.0 | 10 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.17 | 0.18 | 0.10 | 4059 (108) | 0 |
