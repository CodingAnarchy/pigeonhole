**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.03 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 5956570 | 16 | 90.6K | 15.0 | 1753 | 2818 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2688319 | 16 | 32.4K | 24.8 | 3572 | 4424 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4093246 | 16 | 85.4K | 13.8 | 3162 | 18743 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3008 MiB | 0 | 315110 | 6.05 | 31.4 | 48.9 | 3257632 | 7.87 | 1188 | 2179 |
| sparse-wide-deletes | rocksdb | 2153 MiB | 0 | 226992 | 20.4 | 3572 | 4227 | 1385357 | 24.3 | 3604 | 4456 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 273582 | 8.83 | 42.2 | 75.8 | 2181408 | 8.03 | 40.7 | 73.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 149009 | 32.6 | 13.4 | 655 | 2114 |
| sparse-wide-deletes | pigeonhole | get | 2380993 | 11.2 | 5.31 | 90.1 | 199 |
| sparse-wide-deletes | pigeonhole | put | 893699 | 34.4 | 14.8 | 668 | 2081 |
| sparse-wide-deletes | pigeonhole | row delete | 149159 | 35.6 | 16.3 | 664 | 2081 |
| sparse-wide-deletes | pigeonhole | row read | 1191749 | 122 | 19.3 | 1630 | 2540 |
| sparse-wide-deletes | pigeonhole | scan | 1191961 | 691 | 578 | 2408 | 3391 |
| sparse-wide-deletes | rocksdb | column delete | 67283 | 11.0 | 9.34 | 28.9 | 47.4 |
| sparse-wide-deletes | rocksdb | get | 1073854 | 556 | 12.0 | 3523 | 4178 |
| sparse-wide-deletes | rocksdb | put | 403591 | 13.0 | 11.5 | 31.5 | 51.5 |
| sparse-wide-deletes | rocksdb | row delete | 67539 | 14.2 | 12.6 | 34.8 | 56.8 |
| sparse-wide-deletes | rocksdb | row read | 538495 | 649 | 66.0 | 3752 | 4719 |
| sparse-wide-deletes | rocksdb | scan | 537557 | 688 | 110 | 3785 | 4719 |
| sparse-wide-deletes | sqlite-eav | column delete | 102400 | 799 | 11.6 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | get | 1635435 | 10.4 | 7.42 | 39.2 | 73.2 |
| sparse-wide-deletes | sqlite-eav | put | 614955 | 850 | 26.8 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 102741 | 866 | 28.8 | 18350 | 78643 |
| sparse-wide-deletes | sqlite-eav | row read | 819555 | 12.5 | 9.66 | 43.0 | 75.8 |
| sparse-wide-deletes | sqlite-eav | scan | 818160 | 38.7 | 34.8 | 88.1 | 121 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 190483 | 16.0% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 510779 | 42.9% | 7 → 7 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 338203 | 28.4% | 12 → 14 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 152385 | 12.8% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 6 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 11 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 5 | 473.9 | 19 | 4 | 1 (20) | 0 | 0 | 0 | 0.0 | 0.17 | 0.17 | 0.01 | 258 (0) | 0 |
