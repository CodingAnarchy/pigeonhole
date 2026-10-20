**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.45 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 17645030 | 16 | 157.6K | 6.40 | 2966 | 10879 |
| counters | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 32838451 | 16 | 538.6K | 17.2 | 389 | 1450 |
| counters | sqlite-eav | page_cache=320MiB buffered | 1000000 | 5135046 | 16 | 84.7K | 9.41 | 36.1 | 33423 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| counters | pigeonhole | counter read | 1765530 | 855 | 23.7 | 10813 | 16777 |
| counters | pigeonhole | incr | 15879500 | 12.6 | 6.21 | 20.2 | 1786 |
| counters | rocksdb | counter read | 3285070 | 126 | 10.9 | 1450 | 3293 |
| counters | rocksdb | incr | 29553381 | 17.1 | 17.4 | 31.5 | 37.6 |
| counters | sqlite-eav | counter read | 513943 | 4.97 | 4.74 | 12.9 | 31.7 |
| counters | sqlite-eav | incr | 4621103 | 200 | 9.47 | 46.6 | 33423 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 6776531 | 42.7% | 2 → 14 | 0 | 0 | 11 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 4640017 | 29.2% | 2 → 9 | 0 | 0 | 10 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1397871 | 8.8% | 1 → 0 | 0 | 0 | 15 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 790911 | 5.0% | 1 → 0 | 0 | 0 | 5 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 156689 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 213619 | 1.3% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 156703 | 1.0% | 1 → 0 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 192752 | 1.2% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 277281 | 1.7% | 2 → 0 | 1 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 161187 | 1.0% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 210087 | 1.3% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 170729 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 146017 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 216811 | 1.4% | 1 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 148470 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 223825 | 1.4% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | 16 | 12 | 568.8 | 106 | 54 | 11 (97) | 0 | 0 | 2 | 0.8 | 0.06 | 0.06 | 0.01 | 2469 (124) | 0 |
