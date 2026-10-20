**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.52 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 4951348 | 16 | 90.1K | 27.1 | 1696 | 2802 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2079508 | 16 | 39.1K | 78.8 | 3506 | 4358 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2558716 | 16 | 53.7K | 9.98 | 3211 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3072 MiB | 0 | 294998 | 6.14 | 32.3 | 51.5 | 2675384 | 8.00 | 1073 | 2212 |
| sparse-wide-deletes | rocksdb | 2126 MiB | 0 | 199630 | 16.9 | 3506 | 4129 | 1047082 | 17.3 | 3555 | 4424 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 221570 | 7.93 | 32.1 | 55.8 | 1313045 | 7.20 | 31.0 | 52.5 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 123732 | 85.7 | 55.3 | 766 | 2589 |
| sparse-wide-deletes | pigeonhole | get | 1978840 | 11.0 | 5.31 | 90.1 | 197 |
| sparse-wide-deletes | pigeonhole | put | 742806 | 88.3 | 56.6 | 786 | 2654 |
| sparse-wide-deletes | pigeonhole | row delete | 123998 | 87.1 | 57.1 | 774 | 2556 |
| sparse-wide-deletes | pigeonhole | row read | 991542 | 110 | 19.8 | 1630 | 2523 |
| sparse-wide-deletes | pigeonhole | scan | 990430 | 658 | 553 | 2359 | 3293 |
| sparse-wide-deletes | rocksdb | column delete | 52050 | 158 | 143 | 573 | 1933 |
| sparse-wide-deletes | rocksdb | get | 830647 | 417 | 9.73 | 3490 | 4129 |
| sparse-wide-deletes | rocksdb | put | 312168 | 161 | 146 | 614 | 1884 |
| sparse-wide-deletes | rocksdb | row delete | 52180 | 161 | 146 | 594 | 1999 |
| sparse-wide-deletes | rocksdb | row read | 416065 | 502 | 36.1 | 3686 | 4653 |
| sparse-wide-deletes | rocksdb | scan | 416398 | 540 | 88.1 | 3703 | 4653 |
| sparse-wide-deletes | sqlite-eav | column delete | 64021 | 1343 | 9.54 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | get | 1022074 | 7.93 | 6.94 | 30.0 | 51.7 |
| sparse-wide-deletes | sqlite-eav | put | 384043 | 1397 | 50.9 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row delete | 64242 | 1418 | 56.1 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | row read | 512541 | 9.80 | 8.70 | 33.3 | 54.8 |
| sparse-wide-deletes | sqlite-eav | scan | 511795 | 32.8 | 31.0 | 68.6 | 93.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 439701 | 44.4% | 11 → 11 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 337573 | 34.1% | 15 → 15 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 205549 | 20.8% | 4 → 11 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 7623 | 0.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 16 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 17 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 15 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 10 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 7 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 17 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 7 | 473.1 | 24 | 5 | 2 (95) | 0 | 0 | 0 | 0.0 | 0.14 | 0.25 | 0.20 | 764 (14) | 0 |
