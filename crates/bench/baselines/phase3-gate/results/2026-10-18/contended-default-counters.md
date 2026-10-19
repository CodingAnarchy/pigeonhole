**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.75 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 9601396 | 16 | 109.9K | 56.3 | 2769 | 8323 |
| counters | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 7204824 | 16 | 105.9K | 153 | 504 | 1745 |
| counters | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1637491 | 16 | 27.0K | 35.8 | 71.7 | 3228 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| counters | pigeonhole | counter read | 960911 | 692 | 28.2 | 8159 | 12255 |
| counters | pigeonhole | incr | 8640485 | 81.6 | 56.8 | 614 | 2998 |
| counters | rocksdb | counter read | 721010 | 138 | 13.9 | 1638 | 3146 |
| counters | rocksdb | incr | 6483814 | 152 | 155 | 212 | 774 |
| counters | sqlite-eav | counter read | 164017 | 4.65 | 4.74 | 8.77 | 19.3 |
| counters | sqlite-eav | incr | 1473474 | 562 | 36.1 | 72.7 | 3277 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1231376 | 14.3% | 1 → 3 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1100993 | 12.7% | 2 → 4 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1115418 | 12.9% | 1 → 4 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 1202131 | 13.9% | 1 → 2 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1311899 | 15.2% | 1 → 2 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1238833 | 14.3% | 1 → 2 | 0 | 0 | 12 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 377941 | 4.4% | 1 → 2 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 209489 | 2.4% | 1 → 0 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 88922 | 1.0% | 1 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 64523 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 70147 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 179185 | 2.1% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 119573 | 1.4% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 118295 | 1.4% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 50061 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 161699 | 1.9% | 2 → 0 | 0 | 0 | 2 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | 16 | 0 | 0.0 | 61 | 19 | 1 (4) | 0 | 0 | 2 | 0.7 | 0.60 | 0.98 | 0.90 | 22525 (1211) | 0 |
