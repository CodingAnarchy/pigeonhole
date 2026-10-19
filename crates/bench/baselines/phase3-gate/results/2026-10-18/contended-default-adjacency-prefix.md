**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.87 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 14671521 | 16 | 112.2K | 51.2 | 1606 | 2851 |
| adjacency-prefix | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 6500212 | 16 | 49.1K | 84.5 | 2720 | 3883 |
| adjacency-prefix | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2684199 | 16 | 26.5K | 176 | 4358 | 18743 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | filtered scan | 11738698 | 108 | 34.3 | 905 | 2056 |
| adjacency-prefix | pigeonhole | put | 1466816 | 177 | 67.6 | 2507 | 3949 |
| adjacency-prefix | pigeonhole | scan | 1466007 | 386 | 129 | 2294 | 3047 |
| adjacency-prefix | rocksdb | filtered scan | 5199851 | 326 | 64.3 | 2671 | 3817 |
| adjacency-prefix | rocksdb | put | 650233 | 262 | 103 | 3162 | 4686 |
| adjacency-prefix | rocksdb | scan | 650128 | 389 | 143 | 2753 | 3916 |
| adjacency-prefix | sqlite-eav | filtered scan | 2147592 | 496 | 167 | 3949 | 5505 |
| adjacency-prefix | sqlite-eav | put | 268468 | 1596 | 248 | 18743 | 79167 |
| adjacency-prefix | sqlite-eav | scan | 268139 | 422 | 204 | 2966 | 4555 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 46001 | 3.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 36407 | 2.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 85649 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 94225 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 105239 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 78297 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 177466 | 12.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 123265 | 8.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 66053 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 68058 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 35771 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 78138 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 53056 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 74250 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 57927 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| adjacency-prefix | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 287014 | 19.6% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| adjacency-prefix | pigeonhole | 16 | 0 | 0.0 | 8 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.18 | 0.18 | 0.10 | 4030 (100) | 0 |
