**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.41 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 4866801 | 16 | 102.2K | 23.7 | 1327 | 2327 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2059209 | 16 | 40.6K | 77.8 | 3375 | 4194 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2615616 | 16 | 54.1K | 9.79 | 3211 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3328 MiB | 0 | 293154 | 5.66 | 30.7 | 47.1 | 2626529 | 6.88 | 733 | 1745 |
| sparse-wide-deletes | rocksdb | 2127 MiB | 0 | 198614 | 16.9 | 3359 | 3965 | 1036019 | 17.2 | 3408 | 4227 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 223921 | 7.87 | 30.3 | 50.4 | 1344805 | 7.13 | 29.3 | 48.9 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 121594 | 119 | 62.0 | 1040 | 3523 |
| sparse-wide-deletes | pigeonhole | get | 1945261 | 8.76 | 4.77 | 62.0 | 136 |
| sparse-wide-deletes | pigeonhole | put | 730121 | 117 | 63.2 | 1040 | 3310 |
| sparse-wide-deletes | pigeonhole | row delete | 121941 | 118 | 64.5 | 1032 | 3359 |
| sparse-wide-deletes | pigeonhole | row read | 974422 | 81.3 | 18.6 | 1106 | 2245 |
| sparse-wide-deletes | pigeonhole | scan | 973462 | 556 | 481 | 1810 | 2687 |
| sparse-wide-deletes | rocksdb | column delete | 51541 | 159 | 145 | 565 | 1843 |
| sparse-wide-deletes | rocksdb | get | 822601 | 400 | 9.73 | 3342 | 3981 |
| sparse-wide-deletes | rocksdb | put | 309087 | 161 | 150 | 549 | 1712 |
| sparse-wide-deletes | rocksdb | row delete | 51677 | 161 | 148 | 553 | 1688 |
| sparse-wide-deletes | rocksdb | row read | 412032 | 484 | 35.6 | 3539 | 4456 |
| sparse-wide-deletes | rocksdb | scan | 412271 | 518 | 86.5 | 3572 | 4456 |
| sparse-wide-deletes | sqlite-eav | column delete | 65508 | 1362 | 9.41 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | get | 1044825 | 7.81 | 6.88 | 28.3 | 48.1 |
| sparse-wide-deletes | sqlite-eav | put | 392525 | 1393 | 50.2 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | row delete | 65732 | 1313 | 55.3 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row read | 523901 | 9.58 | 8.57 | 31.4 | 50.4 |
| sparse-wide-deletes | sqlite-eav | scan | 523125 | 31.9 | 30.2 | 66.6 | 91.1 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 439735 | 45.2% | 13 → 15 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 532721 | 54.7% | 7 → 7 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1200 | 0.1% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 10 | 573.2 | 19 | 14 | 2 (77) | 0 | 0 | 0 | 0.0 | 0.18 | 0.25 | 0.20 | 378 (2) | 0 |
