**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.21 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 9060914 | 16 | 71.8K | 44.3 | 2376 | 3424 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 10314547 | 16 | 58.9K | 49.7 | 3572 | 6029 |
| sparse-wide | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2867413 | 16 | 34.0K | 30.7 | 8290 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 4096 MiB | 0 | 358866 | 6.78 | 54.0 | 153 | 5074694 | 11.4 | 2212 | 3228 |
| sparse-wide | rocksdb | 2435 MiB | 0 | 371250 | 16.4 | 41.5 | 53.5 | 5813447 | 17.0 | 3310 | 5669 |
| sparse-wide | sqlite-eav | 2813 MiB | 0 | 233932 | 9.66 | 49.7 | 81.9 | 1485969 | 11.6 | 1057 | 1655 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3621680 | 9.01 | 6.69 | 31.9 | 68.1 |
| sparse-wide | pigeonhole | put | 1813776 | 100.0 | 60.2 | 950 | 2818 |
| sparse-wide | pigeonhole | row read | 1811880 | 370 | 59.9 | 2703 | 3604 |
| sparse-wide | pigeonhole | scan | 1813578 | 559 | 317 | 2867 | 3785 |
| sparse-wide | rocksdb | get | 4122457 | 11.6 | 10.4 | 35.1 | 44.3 |
| sparse-wide | rocksdb | put | 2064719 | 225 | 141 | 2605 | 3932 |
| sparse-wide | rocksdb | row read | 2062240 | 524 | 57.6 | 4456 | 6816 |
| sparse-wide | rocksdb | scan | 2065131 | 584 | 108 | 4620 | 7078 |
| sparse-wide | sqlite-eav | get | 1146644 | 12.8 | 8.77 | 50.2 | 82.4 |
| sparse-wide | sqlite-eav | put | 573542 | 1922 | 84.0 | 33554 | 103809 |
| sparse-wide | sqlite-eav | row read | 573257 | 157 | 26.8 | 1303 | 1851 |
| sparse-wide | sqlite-eav | scan | 573970 | 202 | 59.4 | 1483 | 2073 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 777895 | 42.9% | 1 → 6 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 771834 | 42.6% | 2 → 6 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 162202 | 8.9% | 1 → 0 | 0 | 0 | 7 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 16649 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 17301 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 14783 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 13372 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 12556 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 11415 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 15769 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 10 | 379.7 | 46 | 17 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.25 | 0.30 | 0.20 | 1392 (17) | 0 |
