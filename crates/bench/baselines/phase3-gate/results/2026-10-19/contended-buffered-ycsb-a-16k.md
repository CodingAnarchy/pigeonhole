**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 8.82 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 50000 | 5178101 | 16 | 111.8K | 40.4 | 651 | 21365 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 50000 | 3814090 | 16 | 37.2K | 202 | 2023 | 15794 |
| ycsb-a | sqlite-eav | page_cache=320MiB buffered | 50000 | 531335 | 16 | 11.1K | 131 | 33423 | 128975 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 51968 MiB | 0 | 35825 | 186 | 2130 | 9961 | 2552237 | 84.0 | 688 | 4112 |
| ycsb-a | rocksdb | 11658 MiB | 0 | 35733 | 205 | 467 | 635 | 1870679 | 473 | 2081 | 2687 |
| ycsb-a | sqlite-eav | 8107 MiB | 0 | 26719 | 141 | 582 | 733 | 239159 | 136 | 541 | 692 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 2590039 | 143 | 24.7 | 502 | 32375 |
| ycsb-a | pigeonhole | row read | 2588062 | 141 | 85.0 | 717 | 4227 |
| ycsb-a | rocksdb | put | 1907678 | 188 | 61.7 | 618 | 16253 |
| ycsb-a | rocksdb | row read | 1906412 | 664 | 467 | 2073 | 2687 |
| ycsb-a | sqlite-eav | put | 265457 | 2658 | 99.3 | 53740 | 179306 |
| ycsb-a | sqlite-eav | row read | 265878 | 151 | 137 | 549 | 700 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 758334 | 29.3% | 0 → 8 | 0 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 299856 | 11.6% | 1 → 1 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 140105 | 5.4% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 100427 | 3.9% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 135254 | 5.2% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 156097 | 6.0% | 0 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 294767 | 11.4% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 27103 | 1.0% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 170735 | 6.6% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 23127 | 0.9% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 39654 | 1.5% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 85546 | 3.3% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 220656 | 8.5% | 0 → 1 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 25399 | 1.0% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 49432 | 1.9% | 0 → 0 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 63547 | 2.5% | 0 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 1434 | 16826.6 | 2706 | 939 | 57 (181) | 0 | 0 | 3 | 17.3 | 0.29 | 0.30 | 0.04 | 28806 (551) | 0 |
