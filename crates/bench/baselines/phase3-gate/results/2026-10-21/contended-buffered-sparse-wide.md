**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.66 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 8833623 | 16 | 78.9K | 13.5 | 2376 | 3523 |
| sparse-wide | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 12917424 | 16 | 76.1K | 15.4 | 3047 | 4686 |
| sparse-wide | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4433030 | 16 | 41.5K | 33.8 | 3555 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 4352 MiB | 0 | 356367 | 6.27 | 53.5 | 119 | 4940932 | 11.2 | 2277 | 3424 |
| sparse-wide | rocksdb | 2510 MiB | 0 | 390820 | 14.0 | 35.1 | 43.8 | 7356027 | 15.2 | 2867 | 4588 |
| sparse-wide | sqlite-eav | 2874 MiB | 0 | 282646 | 14.3 | 70.7 | 114 | 2377097 | 18.7 | 1851 | 3031 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | get | 3530933 | 8.73 | 6.40 | 31.1 | 68.6 |
| sparse-wide | pigeonhole | put | 1768085 | 14.9 | 9.28 | 26.4 | 995 |
| sparse-wide | pigeonhole | row read | 1766366 | 356 | 54.3 | 2851 | 3817 |
| sparse-wide | pigeonhole | scan | 1768239 | 558 | 330 | 2916 | 3932 |
| sparse-wide | rocksdb | get | 5163285 | 10.2 | 9.09 | 30.1 | 38.1 |
| sparse-wide | rocksdb | put | 2584671 | 7.72 | 7.17 | 16.9 | 23.6 |
| sparse-wide | rocksdb | row read | 2583562 | 484 | 55.6 | 3703 | 4882 |
| sparse-wide | rocksdb | scan | 2585906 | 538 | 98.3 | 3817 | 5014 |
| sparse-wide | sqlite-eav | get | 1773113 | 18.6 | 14.0 | 74.2 | 122 |
| sparse-wide | sqlite-eav | put | 885863 | 1236 | 65.3 | 18481 | 78643 |
| sparse-wide | sqlite-eav | row read | 886630 | 277 | 41.0 | 2376 | 3572 |
| sparse-wide | sqlite-eav | scan | 887424 | 340 | 86.5 | 2638 | 3850 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1547410 | 87.5% | 1 → 12 | 1 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 102954 | 5.8% | 4 → 0 | 0 | 0 | 4 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 117721 | 6.7% | 6 → 0 | 0 | 0 | 6 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide | pigeonhole | 16 | 17 | 613.7 | 50 | 26 | 3 (11) | 0 | 0 | 0 | 0.0 | 0.03 | 0.03 | 0.00 | 82 (1) | 0 |
