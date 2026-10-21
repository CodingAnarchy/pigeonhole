**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.45 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 3506838 | 16 | 75.2K | 27.8 | 1982 | 3244 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2025613 | 16 | 36.7K | 78.8 | 3555 | 4522 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2532765 | 16 | 53.6K | 9.86 | 3228 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3136 MiB | 0 | 256332 | 6.33 | 31.5 | 60.9 | 1847144 | 7.36 | 1352 | 2572 |
| sparse-wide-deletes | rocksdb | 2120 MiB | 0 | 196973 | 16.6 | 3506 | 4325 | 1017677 | 16.9 | 3604 | 4555 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 220419 | 7.87 | 35.3 | 69.1 | 1298539 | 7.13 | 34.6 | 68.6 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 87647 | 111 | 58.1 | 1483 | 3162 |
| sparse-wide-deletes | pigeonhole | get | 1401047 | 11.9 | 5.18 | 101 | 229 |
| sparse-wide-deletes | pigeonhole | put | 526814 | 113 | 59.4 | 1450 | 3162 |
| sparse-wide-deletes | pigeonhole | row delete | 87803 | 114 | 60.2 | 1417 | 3277 |
| sparse-wide-deletes | pigeonhole | row read | 702429 | 138 | 18.9 | 1901 | 3047 |
| sparse-wide-deletes | pigeonhole | scan | 701098 | 769 | 655 | 2703 | 3785 |
| sparse-wide-deletes | rocksdb | column delete | 50700 | 163 | 145 | 696 | 2408 |
| sparse-wide-deletes | rocksdb | get | 809222 | 447 | 9.66 | 3506 | 4293 |
| sparse-wide-deletes | rocksdb | put | 304015 | 165 | 148 | 664 | 2146 |
| sparse-wide-deletes | rocksdb | row delete | 50856 | 165 | 148 | 692 | 2261 |
| sparse-wide-deletes | rocksdb | row read | 405428 | 538 | 36.6 | 3752 | 4784 |
| sparse-wide-deletes | rocksdb | scan | 405392 | 579 | 88.1 | 3785 | 4784 |
| sparse-wide-deletes | sqlite-eav | column delete | 63441 | 1308 | 9.41 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | get | 1011678 | 8.02 | 6.88 | 33.0 | 67.1 |
| sparse-wide-deletes | sqlite-eav | put | 380152 | 1409 | 50.7 | 33423 | 103809 |
| sparse-wide-deletes | sqlite-eav | row delete | 63573 | 1395 | 56.3 | 33423 | 128975 |
| sparse-wide-deletes | sqlite-eav | row read | 507280 | 9.87 | 8.64 | 37.1 | 71.2 |
| sparse-wide-deletes | sqlite-eav | scan | 506641 | 32.5 | 30.3 | 75.3 | 105 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 115837 | 16.5% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 109406 | 15.6% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 216682 | 30.9% | 8 → 9 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 147698 | 21.0% | 5 → 10 | 1 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 112506 | 16.0% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 23 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 63 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 33 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 0 | 0.0 | 20 | 5 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.25 | 0.31 | 0.20 | 657 (6) | 0 |
