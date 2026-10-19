**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 2.71 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 19622613 | 16 | 111.6K | 7.58 | 4948 | 14352 |
| counters | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 33025464 | 16 | 540.1K | 16.8 | 389 | 1458 |
| counters | sqlite-eav | page_cache=320MiB buffered | 1000000 | 5124634 | 16 | 84.7K | 9.41 | 38.9 | 33423 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| counters | pigeonhole | counter read | 1963146 | 1291 | 37.1 | 14221 | 19137 |
| counters | pigeonhole | incr | 17659467 | 15.7 | 7.10 | 21.6 | 2146 |
| counters | rocksdb | counter read | 3303718 | 127 | 10.9 | 1458 | 3539 |
| counters | rocksdb | incr | 29721746 | 16.6 | 16.9 | 31.4 | 37.4 |
| counters | sqlite-eav | counter read | 512870 | 4.96 | 4.74 | 12.9 | 33.5 |
| counters | sqlite-eav | incr | 4611764 | 199 | 9.47 | 48.6 | 33423 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 4264564 | 24.1% | 1 → 9 | 0 | 0 | 7 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 4026698 | 22.8% | 3 → 10 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4033010 | 22.8% | 1 → 6 | 0 | 0 | 0 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2035512 | 11.5% | 1 → 0 | 0 | 0 | 7 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1369303 | 7.8% | 1 → 0 | 0 | 0 | 5 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 453641 | 2.6% | 1 → 0 | 1 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 137655 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 128584 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 194646 | 1.1% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 138955 | 0.8% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 188786 | 1.1% | 2 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 118883 | 0.7% | 1 → 0 | 0 | 0 | 2 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 131348 | 0.7% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 159540 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 150417 | 0.9% | 3 → 0 | 0 | 0 | 3 |
| counters | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 127925 | 0.7% | 2 → 0 | 0 | 0 | 2 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| counters | pigeonhole | 16 | 11 | 854.5 | 94 | 34 | 13 (48) | 0 | 0 | 4 | 4.9 | 0.26 | 0.26 | 0.01 | 3403 (189) | 0 |
