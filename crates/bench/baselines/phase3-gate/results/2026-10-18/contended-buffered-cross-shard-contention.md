**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.13 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 27754586 | 16 | 455.1K | 25.3 | 106 | 352 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1000000 | 29210292 | 16 | 472.3K | 26.2 | 105 | 274 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1000000 | 26770871 | 16 | 441.0K | 26.2 | 107 | 358 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 42153571 | 16 | 622.5K | 25.1 | 39.2 | 57.3 |
| cross-shard-contention | sqlite-eav | page_cache=320MiB buffered | 1000000 | 2339125 | 16 | 37.5K | 17.4 | 1114 | 78643 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | cross-shard commit | 13872853 | 39.3 | 28.2 | 112 | 469 |
| cross-shard-contention | pigeonhole | put | 13881733 | 30.6 | 21.5 | 100 | 262 |
| cross-shard-contention | pigeonhole | cross-shard commit | 14600475 | 38.2 | 29.1 | 111 | 328 |
| cross-shard-contention | pigeonhole | put | 14609817 | 29.1 | 22.1 | 98.8 | 237 |
| cross-shard-contention | pigeonhole | cross-shard commit | 13381760 | 40.7 | 29.2 | 112 | 473 |
| cross-shard-contention | pigeonhole | put | 13389111 | 31.1 | 22.3 | 100 | 281 |
| cross-shard-contention | rocksdb | cross-shard commit | 21072190 | 26.1 | 25.5 | 41.0 | 60.2 |
| cross-shard-contention | rocksdb | put | 21081381 | 25.1 | 24.8 | 35.8 | 53.8 |
| cross-shard-contention | sqlite-eav | cross-shard commit | 1169491 | 407 | 25.3 | 1245 | 78643 |
| cross-shard-contention | sqlite-eav | put | 1169634 | 388 | 11.6 | 342 | 78643 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 27754586 | 13872853 | 145377 | 0.010 | 157308, 0, 0, 0, 0 | 27899963 | 145377 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 29210292 | 14600475 | 144610 | 0.010 | 157587, 0, 0, 0, 0 | 29354902 | 144610 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 26770871 | 13381760 | 137074 | 0.010 | 149062, 0, 0, 0, 0 | 26907945 | 137074 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1735751 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1736919 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1731298 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1736738 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1731728 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1734691 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1736490 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1732967 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 1732423 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 1735301 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 1733225 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 1736625 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 1734499 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 1733313 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 1735192 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 1737426 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 0 | 1826953 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 1 | 1827910 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 2 | 1822192 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 3 | 1827527 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 4 | 1822344 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 5 | 1825938 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 6 | 1827914 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 7 | 1824090 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 8 | 1823774 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 9 | 1826177 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 10 | 1824674 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 11 | 1827631 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 12 | 1825430 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 13 | 1824197 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 14 | 1825881 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered | 15 | 1827660 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 0 | 1673962 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 1 | 1675452 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 2 | 1669319 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 3 | 1675167 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 4 | 1670448 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 5 | 1672895 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 6 | 1674866 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 7 | 1671548 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 8 | 1671125 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 9 | 1673904 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 10 | 1672226 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 11 | 1675048 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 12 | 1673224 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 13 | 1671894 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 14 | 1673837 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on packing=off | 15 | 1675956 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 576 | 144 | 0 (0) | 0 | 0 | 10 | 61.1 | 0.06 | 0.21 | 0.03 | 780344 (273847) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 608 | 160 | 0 (0) | 0 | 0 | 8 | 32.1 | 0.06 | 0.21 | 0.03 | 792191 (279937) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 560 | 144 | 0 (0) | 0 | 0 | 11 | 71.2 | 0.07 | 0.21 | 0.03 | 774372 (264327) | 0 |
