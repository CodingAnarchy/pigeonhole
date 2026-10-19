**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.88 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 6146682 | 16 | 82.0K | 16.4 | 1901 | 3080 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2766369 | 16 | 32.9K | 25.2 | 3424 | 4293 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4101986 | 16 | 85.7K | 13.6 | 3162 | 33292 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 2880 MiB | 0 | 318545 | 6.27 | 33.3 | 51.2 | 3368257 | 7.49 | 1262 | 2376 |
| sparse-wide-deletes | rocksdb | 2158 MiB | 0 | 230064 | 20.5 | 3391 | 4080 | 1428926 | 24.7 | 3457 | 4293 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 273825 | 8.70 | 42.8 | 75.3 | 2186506 | 7.87 | 41.0 | 74.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 153753 | 63.2 | 15.4 | 1335 | 2720 |
| sparse-wide-deletes | pigeonhole | get | 2456915 | 11.4 | 5.31 | 89.6 | 211 |
| sparse-wide-deletes | pigeonhole | put | 922214 | 64.5 | 17.2 | 1360 | 2720 |
| sparse-wide-deletes | pigeonhole | row delete | 153987 | 68.0 | 18.7 | 1393 | 2785 |
| sparse-wide-deletes | pigeonhole | row read | 1229887 | 136 | 20.1 | 1737 | 2867 |
| sparse-wide-deletes | pigeonhole | scan | 1229926 | 734 | 606 | 2605 | 3785 |
| sparse-wide-deletes | rocksdb | column delete | 69194 | 11.7 | 9.54 | 30.1 | 219 |
| sparse-wide-deletes | rocksdb | get | 1104811 | 545 | 12.0 | 3375 | 4047 |
| sparse-wide-deletes | rocksdb | put | 415494 | 13.4 | 11.7 | 32.4 | 176 |
| sparse-wide-deletes | rocksdb | row delete | 69454 | 14.4 | 12.6 | 35.8 | 141 |
| sparse-wide-deletes | rocksdb | row read | 554179 | 640 | 67.6 | 3588 | 4555 |
| sparse-wide-deletes | rocksdb | scan | 553237 | 677 | 111 | 3621 | 4555 |
| sparse-wide-deletes | sqlite-eav | column delete | 102611 | 819 | 11.4 | 18350 | 78643 |
| sparse-wide-deletes | sqlite-eav | get | 1639045 | 10.4 | 7.29 | 39.4 | 72.7 |
| sparse-wide-deletes | sqlite-eav | put | 616216 | 844 | 26.6 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 102958 | 854 | 28.4 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row read | 821286 | 12.4 | 9.47 | 43.8 | 76.3 |
| sparse-wide-deletes | sqlite-eav | scan | 819870 | 38.0 | 34.3 | 87.0 | 120 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 329989 | 26.8% | 11 → 11 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 71261 | 5.8% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 98778 | 8.0% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 125329 | 10.2% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 205557 | 16.7% | 6 → 6 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1845 | 0.2% | 4 → 4 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 899 | 0.1% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 93320 | 7.6% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 2170 | 0.2% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2036 | 0.2% | 4 → 4 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 2528 | 0.2% | 2 → 2 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 2729 | 0.2% | 4 → 4 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1697 | 0.1% | 3 → 3 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 423 | 0.0% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 103575 | 8.4% | 1 → 1 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 187818 | 15.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 2 | 214.0 | 18 | 3 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.18 | 0.18 | 0.03 | 933 (6) | 0 |
