**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.93 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 10180826 | 16 | 82.0K | 50.9 | 3719 | 17433 |
| counters | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7248840 | 16 | 106.8K | 152 | 522 | 1868 |
| counters | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1681130 | 16 | 27.3K | 35.8 | 72.2 | 3228 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| counters | pigeonhole | counter read | 1018644 | 1223 | 33.0 | 17170 | 24642 |
| counters | pigeonhole | incr | 9162182 | 78.9 | 51.2 | 713 | 3064 |
| counters | rocksdb | counter read | 725543 | 144 | 14.0 | 1769 | 3342 |
| counters | rocksdb | incr | 6523297 | 150 | 153 | 207 | 758 |
| counters | sqlite-eav | counter read | 168348 | 4.61 | 4.77 | 8.70 | 18.8 |
| counters | sqlite-eav | incr | 1512782 | 556 | 36.1 | 73.2 | 3277 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1200641 | 13.1% | 1 → 2 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1070567 | 11.7% | 2 → 3 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1129459 | 12.3% | 1 → 2 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 1063928 | 11.6% | 2 → 4 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1125181 | 12.3% | 1 → 4 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1241255 | 13.5% | 1 → 3 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 726269 | 7.9% | 1 → 0 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 328295 | 3.6% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 352860 | 3.9% | 1 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 131174 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 210416 | 2.3% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 194476 | 2.1% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 69510 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 167055 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 71340 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 79756 | 0.9% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | 16 | 0 | 0.0 | 45 | 19 | 0 (0) | 0 | 0 | 2 | 0.2 | 0.31 | 0.91 | 0.90 | 29453 (1615) | 0 |
