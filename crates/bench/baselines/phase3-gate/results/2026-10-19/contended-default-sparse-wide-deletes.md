**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.33 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 3572447 | 16 | 70.0K | 30.7 | 2089 | 3375 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 1996881 | 16 | 37.5K | 78.8 | 3490 | 4424 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2629785 | 16 | 53.9K | 9.86 | 3211 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 2944 MiB | 0 | 258434 | 6.59 | 33.0 | 54.5 | 1884299 | 7.46 | 1376 | 2769 |
| sparse-wide-deletes | rocksdb | 2120 MiB | 0 | 195511 | 16.9 | 3441 | 4096 | 1001802 | 17.2 | 3523 | 4456 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 224507 | 7.90 | 30.7 | 50.2 | 1352633 | 7.13 | 29.4 | 49.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 89350 | 116 | 59.6 | 1532 | 3015 |
| sparse-wide-deletes | pigeonhole | get | 1427373 | 12.8 | 5.41 | 113 | 264 |
| sparse-wide-deletes | pigeonhole | put | 536775 | 115 | 60.7 | 1491 | 2966 |
| sparse-wide-deletes | pigeonhole | row delete | 89453 | 117 | 62.0 | 1450 | 2982 |
| sparse-wide-deletes | pigeonhole | row read | 715360 | 154 | 20.0 | 1974 | 3457 |
| sparse-wide-deletes | pigeonhole | scan | 714136 | 831 | 709 | 2818 | 4096 |
| sparse-wide-deletes | rocksdb | column delete | 49945 | 161 | 145 | 623 | 1901 |
| sparse-wide-deletes | rocksdb | get | 797682 | 437 | 9.86 | 3441 | 4129 |
| sparse-wide-deletes | rocksdb | put | 299854 | 164 | 148 | 643 | 2146 |
| sparse-wide-deletes | rocksdb | row delete | 50157 | 165 | 148 | 659 | 2261 |
| sparse-wide-deletes | rocksdb | row read | 399631 | 527 | 36.6 | 3686 | 4653 |
| sparse-wide-deletes | rocksdb | scan | 399612 | 563 | 87.6 | 3703 | 4653 |
| sparse-wide-deletes | sqlite-eav | column delete | 65844 | 1332 | 9.47 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | get | 1050472 | 7.83 | 6.88 | 28.4 | 48.4 |
| sparse-wide-deletes | sqlite-eav | put | 394729 | 1383 | 49.9 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row delete | 66093 | 1410 | 55.3 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | row read | 526668 | 9.66 | 8.64 | 31.6 | 51.7 |
| sparse-wide-deletes | sqlite-eav | scan | 525979 | 32.7 | 30.8 | 67.6 | 92.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 142440 | 19.9% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 112400 | 15.7% | 15 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 245340 | 34.3% | 12 → 12 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 215367 | 30.1% | 5 → 6 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 31 | 0.0% | 2 → 0 | 0 | 0 | 2 |
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
| sparse-wide-deletes | pigeonhole | 16 | 4 | 172.3 | 17 | 1 | 2 (58) | 0 | 0 | 0 | 0.0 | 0.30 | 0.33 | 0.20 | 592 (10) | 0 |
