**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.71 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 37699111 | 16 | 556.5K | 26.1 | 84.0 | 125 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 45926346 | 16 | 638.0K | 21.8 | 70.1 | 108 |
| ycsb-f | sqlite-eav | page_cache=320MiB buffered | 1000000 | 5084165 | 16 | 82.4K | 11.0 | 1106 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2560 MiB | 0 | 443278 | 18.3 | 41.2 | 90.6 | 18403848 | 18.0 | 56.8 | 81.9 |
| ycsb-f | rocksdb | 1519 MiB | 0 | 449459 | 20.1 | 40.7 | 50.9 | 22509562 | 32.6 | 76.8 | 96.8 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 278046 | 9.02 | 30.6 | 56.1 | 2262339 | 8.26 | 29.7 | 52.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 18851985 | 37.9 | 34.0 | 94.2 | 140 |
| ycsb-f | pigeonhole | row read | 18847126 | 18.7 | 18.0 | 56.8 | 82.4 |
| ycsb-f | rocksdb | read-modify-write | 22967325 | 16.0 | 13.2 | 40.7 | 142 |
| ycsb-f | rocksdb | row read | 22959021 | 33.7 | 32.4 | 76.3 | 96.8 |
| ycsb-f | sqlite-eav | read-modify-write | 2543780 | 369 | 13.3 | 3211 | 78643 |
| ycsb-f | sqlite-eav | row read | 2540385 | 9.01 | 8.38 | 29.7 | 53.0 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 2027843 | 10.8% | 1 → 2 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1782572 | 9.5% | 2 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1656705 | 8.8% | 1 → 1 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 1936586 | 10.3% | 2 → 3 | 1 | 0 | 3 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 1696807 | 9.0% | 1 → 2 | 1 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1859473 | 9.9% | 1 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 1620107 | 8.6% | 1 → 2 | 1 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1475353 | 7.8% | 1 → 2 | 2 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 2128557 | 11.3% | 2 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2021691 | 10.7% | 1 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 410318 | 2.2% | 2 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 106365 | 0.6% | 2 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 129608 | 0.7% | 1 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 0 | 0.0 | 205 | 55 | 5 (5) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.50 | 14171 (1409) | 0 |
