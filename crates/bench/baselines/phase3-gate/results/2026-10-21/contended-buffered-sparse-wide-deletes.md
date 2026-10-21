**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.25 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 5477425 | 16 | 97.3K | 13.8 | 1499 | 2376 |
| sparse-wide-deletes | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 2722064 | 16 | 33.0K | 24.6 | 3457 | 4325 |
| sparse-wide-deletes | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4233677 | 16 | 87.3K | 13.4 | 3162 | 18481 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 3072 MiB | 0 | 306095 | 5.79 | 31.5 | 52.0 | 2979419 | 7.29 | 1057 | 2032 |
| sparse-wide-deletes | rocksdb | 2154 MiB | 0 | 228350 | 20.1 | 3441 | 4080 | 1404132 | 23.9 | 3506 | 4358 |
| sparse-wide-deletes | sqlite-eav | 2720 MiB | 0 | 277436 | 8.77 | 38.9 | 73.7 | 2262147 | 7.90 | 37.4 | 71.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | column delete | 136909 | 25.1 | 11.5 | 436 | 1892 |
| sparse-wide-deletes | pigeonhole | get | 2189371 | 10.5 | 4.96 | 82.4 | 184 |
| sparse-wide-deletes | pigeonhole | put | 821812 | 26.9 | 12.9 | 449 | 1901 |
| sparse-wide-deletes | pigeonhole | row delete | 137164 | 28.2 | 14.2 | 446 | 1942 |
| sparse-wide-deletes | pigeonhole | row read | 1096143 | 114 | 18.8 | 1507 | 2408 |
| sparse-wide-deletes | pigeonhole | scan | 1096026 | 648 | 578 | 1974 | 2867 |
| sparse-wide-deletes | rocksdb | column delete | 68095 | 11.3 | 9.60 | 29.1 | 43.8 |
| sparse-wide-deletes | rocksdb | get | 1087299 | 544 | 11.8 | 3408 | 4047 |
| sparse-wide-deletes | rocksdb | put | 408788 | 13.2 | 11.6 | 31.5 | 46.3 |
| sparse-wide-deletes | rocksdb | row delete | 68373 | 14.7 | 13.1 | 36.1 | 50.9 |
| sparse-wide-deletes | rocksdb | row read | 545183 | 636 | 64.0 | 3637 | 4620 |
| sparse-wide-deletes | rocksdb | scan | 544326 | 674 | 109 | 3670 | 4620 |
| sparse-wide-deletes | sqlite-eav | column delete | 105873 | 819 | 11.5 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | get | 1691840 | 10.0 | 7.36 | 36.1 | 70.7 |
| sparse-wide-deletes | sqlite-eav | put | 635812 | 827 | 26.2 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row delete | 106126 | 835 | 27.9 | 18350 | 53477 |
| sparse-wide-deletes | sqlite-eav | row read | 847743 | 12.0 | 9.47 | 39.9 | 73.7 |
| sparse-wide-deletes | sqlite-eav | scan | 846283 | 36.9 | 33.5 | 81.9 | 113 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 194941 | 17.8% | 13 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 269889 | 24.6% | 4 → 10 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 142193 | 13.0% | 15 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 476536 | 43.5% | 15 → 15 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 12194 | 1.1% | 16 → 16 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 30 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 24 | 0.0% | 2 → 0 | 0 | 0 | 2 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 12 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 11 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 26 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 9 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 8 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 12 | 0.0% | 1 → 0 | 0 | 0 | 1 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| sparse-wide-deletes | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| sparse-wide-deletes | pigeonhole | 16 | 5 | 403.0 | 27 | 6 | 1 (56) | 0 | 0 | 0 | 0.0 | 0.11 | 0.11 | 0.01 | 256 (1) | 0 |
