**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 6.28 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 50000 | 6928063 | 16 | 119.0K | 89.6 | 913 | 4047 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 50000 | 1822563 | 16 | 29.3K | 328 | 9175 | 29360 |
| ycsb-a | sqlite-eav | page_cache=320MiB sync load=buffered | 50000 | 413838 | 16 | 8.6K | 137 | 33554 | 179306 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 71424 MiB | 0 | 35843 | 127 | 926 | 3555 | 3428010 | 84.0 | 442 | 2589 |
| ycsb-a | rocksdb | 8966 MiB | 0 | 34527 | 122 | 256 | 338 | 876470 | 168 | 598 | 827 |
| ycsb-a | sqlite-eav | 8107 MiB | 0 | 24344 | 139 | 545 | 700 | 182814 | 135 | 498 | 676 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 3464210 | 154 | 93.2 | 1311 | 6029 |
| ycsb-a | pigeonhole | row read | 3463853 | 113 | 84.5 | 444 | 2605 |
| ycsb-a | rocksdb | put | 911566 | 890 | 494 | 18743 | 36438 |
| ycsb-a | rocksdb | row read | 910997 | 198 | 165 | 594 | 823 |
| ycsb-a | sqlite-eav | put | 206680 | 3433 | 139 | 78643 | 329253 |
| ycsb-a | sqlite-eav | row read | 207158 | 144 | 136 | 504 | 676 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 373746 | 10.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 387733 | 11.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 468003 | 13.5% | 1 → 6 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 211103 | 6.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 83446 | 2.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 162593 | 4.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 91787 | 2.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 157256 | 4.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 209408 | 6.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 738304 | 21.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 243528 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 56344 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 103110 | 3.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 61099 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 49293 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 67457 | 1.9% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 153 | 8170.4 | 3637 | 984 | 25 (49) | 0 | 0 | 0 | 0.0 | 0.30 | 0.56 | 0.50 | 7462 (371) | 0 |
