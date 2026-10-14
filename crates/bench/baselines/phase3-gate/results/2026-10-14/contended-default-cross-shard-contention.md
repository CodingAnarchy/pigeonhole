**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.06 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 5612302 | 16 | 86.5K | 159 | 295 | 3604 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1000000 | 6698682 | 16 | 96.5K | 140 | 332 | 3817 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7001569 | 16 | 108.4K | 162 | 211 | 247 |
| cross-shard-contention | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1164377 | 16 | 18.1K | 44.8 | 108 | 53740 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | cross-shard commit | 2806559 | 198 | 167 | 303 | 3932 |
| cross-shard-contention | pigeonhole | put | 2805743 | 170 | 148 | 281 | 3178 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3350303 | 179 | 148 | 492 | 4194 |
| cross-shard-contention | pigeonhole | put | 3348379 | 152 | 130 | 289 | 3408 |
| cross-shard-contention | rocksdb | cross-shard commit | 3501853 | 158 | 166 | 214 | 252 |
| cross-shard-contention | rocksdb | put | 3499716 | 134 | 155 | 208 | 242 |
| cross-shard-contention | sqlite-eav | cross-shard commit | 582039 | 783 | 53.5 | 123 | 53740 |
| cross-shard-contention | sqlite-eav | put | 582338 | 781 | 38.4 | 85.0 | 78643 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5612302 | 2806559 | 15661 | 0.006 | 16041, 0, 191, 13, 0 | 5627963 | 15661 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6698682 | 3350303 | 13678 | 0.004 | 14465, 0, 0, 0, 0 | 6712360 | 13678 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1360724 | 24.2% | 16 → 7 | 9 | 0 | 9 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 596364 | 10.6% | 5 → 4 | 0 | 0 | 3 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 598287 | 10.7% | 1 → 5 | 0 | 0 | 3 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 619170 | 11.0% | 1 → 5 | 0 | 0 | 2 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 462664 | 8.2% | 0 → 3 | 0 | 0 | 2 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 462444 | 8.2% | 0 → 3 | 0 | 0 | 1 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 443904 | 7.9% | 0 → 4 | 0 | 0 | 1 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 164872 | 2.9% | 0 → 3 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 292632 | 5.2% | 0 → 3 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 303998 | 5.4% | 0 → 3 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 307243 | 5.5% | 0 → 4 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 0 | 419311 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1 | 419846 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 2 | 417591 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 3 | 419554 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 4 | 418247 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 5 | 419287 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6 | 418785 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 7 | 417710 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 8 | 418171 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 9 | 418554 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 10 | 417762 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 11 | 418643 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 12 | 419739 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 13 | 417785 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 14 | 418227 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 15 | 419470 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | 16 | 9 | 493.7 | 138 | 77 | 236 (144) | 0 | 0 | 0 | 0.0 | 0.53 | 1.76 | 1.00 | 3889192 (466345) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 0 | 0.0 | 1.22 | 2.74 | 1.00 | 4582910 (828418) | 0 |
