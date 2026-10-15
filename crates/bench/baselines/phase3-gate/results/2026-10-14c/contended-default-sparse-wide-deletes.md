**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.20 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 4661633 | 16 | 84.0K | 27.6 | 1753 | 2753 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 1982892 | 16 | 36.9K | 79.4 | 3555 | 4456 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2524027 | 16 | 53.8K | 9.86 | 3211 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 2944 MiB | 0 | 288289 | 6.27 | 31.5 | 50.4 | 2508123 | 7.78 | 1106 | 2130 |
| sparse-wide-deletes | rocksdb | 2120 MiB | 0 | 194826 | 16.9 | 3490 | 4227 | 994084 | 17.2 | 3588 | 4489 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 220018 | 7.90 | 30.8 | 52.2 | 1293671 | 7.13 | 29.3 | 49.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 116519 | 100 | 58.4 | 1171 | 2818 |
| sparse-wide-deletes | pigeonhole | get | 1862951 | 11.3 | 5.34 | 89.6 | 195 |
| sparse-wide-deletes | pigeonhole | put | 699596 | 103 | 59.9 | 1163 | 2884 |
| sparse-wide-deletes | pigeonhole | row delete | 116776 | 101 | 60.9 | 1122 | 2736 |
| sparse-wide-deletes | pigeonhole | row read | 933461 | 119 | 19.5 | 1581 | 2507 |
| sparse-wide-deletes | pigeonhole | scan | 932330 | 692 | 582 | 2327 | 3244 |
| sparse-wide-deletes | rocksdb | column delete | 49580 | 161 | 146 | 606 | 1884 |
| sparse-wide-deletes | rocksdb | get | 792114 | 446 | 9.73 | 3506 | 4227 |
| sparse-wide-deletes | rocksdb | put | 297765 | 165 | 150 | 631 | 2130 |
| sparse-wide-deletes | rocksdb | row delete | 49807 | 163 | 148 | 602 | 1925 |
| sparse-wide-deletes | rocksdb | row read | 396796 | 535 | 36.6 | 3736 | 4686 |
| sparse-wide-deletes | rocksdb | scan | 396830 | 572 | 88.1 | 3768 | 4686 |
| sparse-wide-deletes | sqlite-eav | column delete | 63215 | 1316 | 9.47 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | get | 1008191 | 7.85 | 6.91 | 28.3 | 48.9 |
| sparse-wide-deletes | sqlite-eav | put | 378849 | 1402 | 50.4 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row delete | 63364 | 1383 | 55.6 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row read | 505498 | 9.69 | 8.64 | 31.6 | 52.0 |
| sparse-wide-deletes | sqlite-eav | scan | 504910 | 32.7 | 31.0 | 68.1 | 93.2 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 317963 | 34.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 300721 | 32.2% | 8 → 13 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 244003 | 26.2% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 69927 | 7.5% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 13 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 103 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 16 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 124 | 0.0% | 2 → 1 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 21 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 6 | 380.4 | 20 | 3 | 1 (61) | 0 | 0 | 0 | 0.0 | 0.28 | 0.32 | 0.20 | 919 (31) | 0 |
