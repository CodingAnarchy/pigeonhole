**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 10.87 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 50000 | 5595445 | 16 | 118.7K | 39.9 | 852 | 13435 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 50000 | 3618408 | 16 | 36.8K | 211 | 2056 | 15729 |
| ycsb-a | sqlite-eav | page_cache=320MiB buffered | 50000 | 529452 | 16 | 11.1K | 131 | 33423 | 128975 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 55040 MiB | 0 | 35834 | 188 | 2073 | 6881 | 2761165 | 85.5 | 1114 | 5276 |
| ycsb-a | rocksdb | 11961 MiB | 0 | 35691 | 214 | 487 | 713 | 1772663 | 485 | 2130 | 2720 |
| ycsb-a | sqlite-eav | 8107 MiB | 0 | 26678 | 141 | 578 | 733 | 238244 | 136 | 532 | 684 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 2798446 | 109 | 25.0 | 422 | 22151 |
| ycsb-a | pigeonhole | row read | 2796999 | 158 | 87.0 | 1139 | 5308 |
| ycsb-a | rocksdb | put | 1810054 | 183 | 64.3 | 729 | 16253 |
| ycsb-a | rocksdb | row read | 1808354 | 679 | 477 | 2130 | 2720 |
| ycsb-a | sqlite-eav | put | 264530 | 2648 | 99.3 | 53740 | 179306 |
| ycsb-a | sqlite-eav | row read | 264922 | 151 | 137 | 537 | 692 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 379139 | 13.5% | 0 → 8 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 159137 | 5.7% | 1 → 1 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 232010 | 8.3% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 195810 | 7.0% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 105590 | 3.8% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 289028 | 10.3% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 244667 | 8.7% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 222412 | 7.9% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 187002 | 6.7% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 64582 | 2.3% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 317982 | 11.4% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 33349 | 1.2% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 143535 | 5.1% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 43417 | 1.6% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 69823 | 2.5% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 110963 | 4.0% | 0 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 169 | 8728.6 | 2948 | 888 | 16 (37) | 0 | 1 | 3 | 225.7 | 0.34 | 0.35 | 0.04 | 29705 (652) | 0 |
