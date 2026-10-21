**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.17 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 9248871 | 16 | 72.8K | 42.5 | 2556 | 3998 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10452087 | 16 | 59.7K | 50.2 | 3457 | 5898 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2912329 | 16 | 34.1K | 30.1 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 4096 MiB | 0 | 360853 | 6.91 | 48.9 | 100 | 5185253 | 11.7 | 2392 | 3867 |
| sparse-wide | rocksdb | 2444 MiB | 0 | 372421 | 16.6 | 42.2 | 54.0 | 5895093 | 17.3 | 3195 | 5439 |
| sparse-wide | sqlite-eav | 2815 MiB | 0 | 235624 | 9.60 | 48.1 | 81.9 | 1511215 | 11.6 | 1057 | 1671 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3696984 | 9.13 | 6.91 | 31.1 | 57.1 |
| sparse-wide | pigeonhole | put | 1851370 | 105 | 59.9 | 1196 | 3015 |
| sparse-wide | pigeonhole | row read | 1849122 | 380 | 55.3 | 3113 | 4424 |
| sparse-wide | pigeonhole | scan | 1851395 | 555 | 303 | 3162 | 4456 |
| sparse-wide | rocksdb | get | 4177625 | 11.8 | 10.6 | 35.8 | 45.1 |
| sparse-wide | rocksdb | put | 2092148 | 220 | 139 | 2556 | 3883 |
| sparse-wide | rocksdb | row read | 2089889 | 516 | 58.4 | 4260 | 6980 |
| sparse-wide | rocksdb | scan | 2092425 | 578 | 110 | 4456 | 7242 |
| sparse-wide | sqlite-eav | get | 1164703 | 12.8 | 8.83 | 49.2 | 82.9 |
| sparse-wide | sqlite-eav | put | 582539 | 1920 | 82.9 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 582136 | 156 | 26.6 | 1311 | 1884 |
| sparse-wide | sqlite-eav | scan | 582951 | 199 | 57.9 | 1475 | 2089 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 219017 | 11.8% | 6 → 13 | 2 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1054170 | 56.9% | 8 → 15 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 306345 | 16.5% | 6 → 7 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 178380 | 9.6% | 11 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1219 | 0.1% | 10 → 1 | 0 | 0 | 9 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 20232 | 1.1% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 25314 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 14072 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 18647 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 13974 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 8 | 339.3 | 70 | 32 | 6 (39) | 0 | 0 | 0 | 0.0 | 0.18 | 0.27 | 0.20 | 1055 (20) | 0 |
