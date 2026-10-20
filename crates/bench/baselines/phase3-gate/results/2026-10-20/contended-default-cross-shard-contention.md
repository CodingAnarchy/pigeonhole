**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.83 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 6723184 | 16 | 106.9K | 138 | 246 | 1778 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1000000 | 6777510 | 16 | 107.3K | 138 | 245 | 1524 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 1000000 | 6836762 | 16 | 107.5K | 138 | 244 | 1483 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7169062 | 16 | 110.1K | 160 | 209 | 262 |
| cross-shard-contention | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1125468 | 16 | 18.1K | 44.8 | 108 | 53740 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | cross-shard commit | 3362517 | 162 | 147 | 255 | 1958 |
| cross-shard-contention | pigeonhole | put | 3360667 | 137 | 127 | 233 | 1540 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3389508 | 161 | 146 | 253 | 1614 |
| cross-shard-contention | pigeonhole | put | 3388002 | 137 | 127 | 232 | 1450 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3419242 | 161 | 146 | 252 | 1745 |
| cross-shard-contention | pigeonhole | put | 3417520 | 136 | 127 | 230 | 1294 |
| cross-shard-contention | rocksdb | cross-shard commit | 3585321 | 155 | 164 | 212 | 276 |
| cross-shard-contention | rocksdb | put | 3583741 | 130 | 151 | 206 | 247 |
| cross-shard-contention | sqlite-eav | cross-shard commit | 562457 | 800 | 53.5 | 124 | 78643 |
| cross-shard-contention | sqlite-eav | put | 563011 | 724 | 38.4 | 84.5 | 53740 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6723184 | 3362517 | 12407 | 0.004 | 13064, 0, 0, 0, 0 | 6735591 | 12407 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6777510 | 3389508 | 13094 | 0.004 | 13783, 0, 0, 0, 0 | 6790604 | 13094 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 6836762 | 3419242 | 12653 | 0.004 | 13303, 0, 0, 0, 0 | 6849415 | 12653 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 420843 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 421429 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 419115 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 421084 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 419763 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 420798 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 420280 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 419264 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 419736 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 420063 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 419280 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 420211 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 421237 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 419362 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 419730 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 420989 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 0 | 424260 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1 | 424761 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 2 | 422455 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 3 | 424581 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 4 | 423185 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 5 | 424155 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6 | 423681 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 7 | 422589 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 8 | 423162 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 9 | 423375 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 10 | 422784 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 11 | 423556 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 12 | 424593 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 13 | 422779 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 14 | 423189 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 15 | 424405 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 0 | 428007 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 1 | 428631 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 2 | 426173 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 3 | 428342 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 4 | 426772 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 5 | 427750 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 6 | 427363 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 7 | 426232 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 8 | 426938 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 9 | 427051 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 10 | 426605 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 11 | 427283 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 12 | 428294 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 13 | 426452 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 14 | 426882 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 15 | 427987 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 2.07 | 1.00 | 4583215 (893299) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.11 | 2.07 | 1.00 | 4616913 (945995) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.11 | 2.07 | 1.00 | 4657343 (942339) | 0 |
