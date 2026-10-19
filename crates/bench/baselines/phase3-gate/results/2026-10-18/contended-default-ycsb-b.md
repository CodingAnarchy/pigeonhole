**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.27 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 78981373 | 16 | 1.22M | 5.18 | 79.9 | 127 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 40331700 | 16 | 444.0K | 26.9 | 179 | 676 |
| ycsb-b | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 9889659 | 16 | 163.5K | 7.58 | 1130 | 18350 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 2240 MiB | 0 | 459008 | 14.8 | 37.9 | 69.1 | 74568697 | 4.45 | 37.4 | 54.0 |
| ycsb-b | rocksdb | 1296 MiB | 0 | 456653 | 15.8 | 31.9 | 44.0 | 37854803 | 26.2 | 69.1 | 85.5 |
| ycsb-b | sqlite-eav | 1788 MiB | 0 | 404556 | 8.06 | 47.9 | 84.5 | 8989702 | 7.42 | 45.1 | 77.3 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | put | 3953668 | 68.1 | 59.6 | 152 | 342 |
| ycsb-b | pigeonhole | row read | 75027705 | 9.13 | 4.48 | 37.4 | 54.0 |
| ycsb-b | rocksdb | put | 2020244 | 171 | 144 | 950 | 1982 |
| ycsb-b | rocksdb | row read | 38311456 | 28.7 | 26.0 | 69.1 | 85.5 |
| ycsb-b | sqlite-eav | put | 495401 | 1726 | 53.5 | 33423 | 103809 |
| ycsb-b | sqlite-eav | row read | 9394258 | 9.55 | 7.46 | 45.3 | 77.8 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 641378 | 16.2% | 1 → 4 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 667955 | 16.9% | 1 → 2 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 570543 | 14.4% | 1 → 4 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 843955 | 21.3% | 1 → 6 | 0 | 0 | 5 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 666833 | 16.9% | 1 → 0 | 0 | 0 | 3 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 152010 | 3.8% | 1 → 0 | 0 | 0 | 2 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 30735 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 70421 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 44360 | 1.1% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 39194 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 36940 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 38946 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 40472 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 70620 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 21293 | 0.5% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 18013 | 0.5% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 16 | 1 | 18.4 | 50 | 11 | 1 (2) | 0 | 0 | 0 | 0.0 | 0.05 | 0.07 | 0.05 | 6844 (441) | 0 |
