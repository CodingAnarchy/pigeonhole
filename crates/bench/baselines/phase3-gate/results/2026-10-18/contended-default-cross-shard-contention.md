**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.04 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 6694930 | 16 | 103.1K | 139 | 246 | 2998 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1000000 | 6790679 | 16 | 106.4K | 138 | 244 | 1540 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 1000000 | 6838859 | 16 | 104.5K | 138 | 245 | 2523 |
| cross-shard-contention | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7053755 | 16 | 106.6K | 160 | 209 | 261 |
| cross-shard-contention | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1153831 | 16 | 18.2K | 44.8 | 110 | 103809 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | cross-shard commit | 3348434 | 167 | 147 | 254 | 3310 |
| cross-shard-contention | pigeonhole | put | 3346496 | 143 | 129 | 234 | 2687 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3396090 | 162 | 146 | 252 | 1769 |
| cross-shard-contention | pigeonhole | put | 3394589 | 138 | 128 | 232 | 1286 |
| cross-shard-contention | pigeonhole | cross-shard commit | 3420316 | 165 | 146 | 253 | 2785 |
| cross-shard-contention | pigeonhole | put | 3418543 | 141 | 128 | 233 | 2179 |
| cross-shard-contention | rocksdb | cross-shard commit | 3527822 | 156 | 164 | 211 | 272 |
| cross-shard-contention | rocksdb | put | 3525933 | 134 | 155 | 206 | 249 |
| cross-shard-contention | sqlite-eav | cross-shard commit | 576740 | 812 | 53.5 | 130 | 103809 |
| cross-shard-contention | sqlite-eav | put | 577091 | 760 | 38.4 | 84.5 | 128975 |

| Workload | Store | Settings | Commits | Cross-shard commits | Refused rounds | Per cross-shard commit | Refusals: below floor, move raise, moved, room, pending overlap | Seqnos used | Inferred wasted rounds |
|---|---|---|--:|--:|--:|--:|---|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6694930 | 3348434 | 12333 | 0.004 | 13027, 0, 0, 0, 0 | 6707263 | 12333 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6790679 | 3396090 | 12122 | 0.004 | 12851, 0, 0, 0, 0 | 6802801 | 12122 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 6838859 | 3420316 | 12483 | 0.004 | 13163, 0, 0, 0, 0 | 6851342 | 12483 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 419094 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 419634 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 417357 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 419333 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 418020 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 419065 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 418535 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 417437 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 417929 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 418332 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 417541 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 418415 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 419463 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 417545 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 417994 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 419236 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 0 | 425088 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 1 | 425666 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 2 | 423276 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 3 | 425424 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 4 | 423970 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 5 | 424939 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 6 | 424508 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 7 | 423361 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 8 | 423991 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 9 | 424217 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 10 | 423611 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 11 | 424387 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 12 | 425402 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 13 | 423588 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 14 | 424027 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered | 15 | 425224 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 0 | 428124 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 1 | 428760 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 2 | 426307 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 3 | 428461 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 4 | 426904 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 5 | 427866 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 6 | 427477 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 7 | 426363 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 8 | 427070 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 9 | 427202 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 10 | 426740 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 11 | 427410 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 12 | 428443 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 13 | 426569 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 14 | 427023 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| cross-shard-contention | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on packing=off | 15 | 428140 | 6.3% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 0 | 0.0 | 1.19 | 2.65 | 1.00 | 4576483 (892546) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 0 | 0.0 | 1.18 | 2.64 | 1.00 | 4639875 (918025) | 0 |
| cross-shard-contention | pigeonhole | 16 | 0 | 0.0 | 144 | 32 | 0 (0) | 0 | 0 | 1 | 0.1 | 1.18 | 2.64 | 1.00 | 4673624 (903319) | 0 |
