**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.25 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 4876548 | 16 | 78.0K | 30.5 | 1901 | 3047 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2081581 | 16 | 38.5K | 77.8 | 3539 | 4424 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2599677 | 16 | 53.8K | 9.86 | 3211 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 2688 MiB | 0 | 293348 | 6.40 | 33.3 | 50.9 | 2632114 | 7.93 | 1319 | 2376 |
| sparse-wide-deletes | rocksdb | 2125 MiB | 0 | 199721 | 16.8 | 3506 | 4227 | 1048239 | 17.2 | 3588 | 4456 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 223275 | 7.90 | 34.0 | 56.8 | 1335996 | 7.13 | 33.0 | 56.6 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 121841 | 125 | 59.9 | 1688 | 3047 |
| sparse-wide-deletes | pigeonhole | get | 1949098 | 12.2 | 5.54 | 98.3 | 220 |
| sparse-wide-deletes | pigeonhole | put | 731640 | 129 | 61.4 | 1696 | 3097 |
| sparse-wide-deletes | pigeonhole | row delete | 122215 | 128 | 62.5 | 1679 | 3047 |
| sparse-wide-deletes | pigeonhole | row read | 976364 | 140 | 20.9 | 1810 | 2753 |
| sparse-wide-deletes | pigeonhole | scan | 975390 | 722 | 598 | 2540 | 3588 |
| sparse-wide-deletes | rocksdb | column delete | 52101 | 166 | 151 | 639 | 2114 |
| sparse-wide-deletes | rocksdb | get | 831498 | 424 | 9.66 | 3506 | 4227 |
| sparse-wide-deletes | rocksdb | put | 312467 | 169 | 154 | 631 | 2146 |
| sparse-wide-deletes | rocksdb | row delete | 52228 | 167 | 154 | 606 | 1991 |
| sparse-wide-deletes | rocksdb | row read | 416462 | 509 | 36.1 | 3719 | 4686 |
| sparse-wide-deletes | rocksdb | scan | 416825 | 545 | 86.5 | 3752 | 4719 |
| sparse-wide-deletes | sqlite-eav | column delete | 65092 | 1351 | 9.47 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | get | 1038491 | 7.95 | 6.88 | 31.9 | 54.8 |
| sparse-wide-deletes | sqlite-eav | put | 390037 | 1391 | 50.7 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row delete | 65298 | 1434 | 55.8 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row read | 520780 | 9.76 | 8.64 | 35.1 | 59.1 |
| sparse-wide-deletes | sqlite-eav | scan | 519979 | 32.6 | 30.6 | 70.1 | 96.8 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 319640 | 32.8% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 146690 | 15.0% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 77663 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 247657 | 25.4% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 105272 | 10.8% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 78774 | 8.1% | 1 → 1 | 0 | 0 | 0 |
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
| sparse-wide-deletes | pigeonhole | 16 | 0 | 0.0 | 16 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.31 | 0.33 | 0.20 | 1586 (33) | 0 |
