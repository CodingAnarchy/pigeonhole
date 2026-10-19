**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.24 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 8576128 | 16 | 71.1K | 45.8 | 2490 | 3736 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10459712 | 16 | 59.8K | 49.7 | 3473 | 5505 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2935033 | 16 | 34.3K | 30.2 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 3904 MiB | 0 | 353405 | 7.23 | 61.7 | 143 | 4789274 | 11.8 | 2343 | 3539 |
| sparse-wide | rocksdb | 2448 MiB | 0 | 372482 | 16.4 | 41.7 | 53.5 | 5899427 | 17.2 | 3211 | 5210 |
| sparse-wide | sqlite-eav | 2816 MiB | 0 | 236489 | 9.54 | 48.1 | 80.4 | 1524010 | 11.5 | 1032 | 1622 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3427956 | 9.48 | 7.01 | 34.8 | 88.6 |
| sparse-wide | pigeonhole | put | 1716695 | 119 | 62.2 | 1360 | 3146 |
| sparse-wide | pigeonhole | row read | 1714723 | 378 | 59.6 | 2933 | 3949 |
| sparse-wide | pigeonhole | scan | 1716754 | 561 | 317 | 3080 | 4145 |
| sparse-wide | rocksdb | get | 4180417 | 11.6 | 10.6 | 34.6 | 44.0 |
| sparse-wide | rocksdb | put | 2093758 | 216 | 139 | 2507 | 3883 |
| sparse-wide | rocksdb | row read | 2091492 | 519 | 58.1 | 4293 | 6259 |
| sparse-wide | rocksdb | scan | 2094045 | 578 | 109 | 4456 | 6455 |
| sparse-wide | sqlite-eav | get | 1173781 | 12.8 | 8.77 | 48.9 | 80.9 |
| sparse-wide | sqlite-eav | put | 587125 | 1918 | 82.9 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 586718 | 154 | 26.8 | 1270 | 1819 |
| sparse-wide | sqlite-eav | scan | 587409 | 199 | 58.1 | 1458 | 2064 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 270821 | 15.8% | 14 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 308997 | 18.0% | 3 → 8 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 196551 | 11.4% | 6 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 780864 | 45.5% | 3 → 14 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 11357 | 0.7% | 3 → 16 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1363 | 0.1% | 7 → 0 | 0 | 0 | 7 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 25774 | 1.5% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 1467 | 0.1% | 8 → 0 | 0 | 0 | 8 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 712 | 0.0% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 582 | 0.0% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 455 | 0.0% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 3204 | 0.2% | 5 → 1 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 21427 | 1.2% | 3 → 2 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 29890 | 1.7% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 22705 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 40526 | 2.4% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 5 | 180.7 | 83 | 21 | 3 (30) | 0 | 0 | 0 | 0.0 | 0.28 | 0.32 | 0.20 | 3183 (57) | 654 |
