**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.95 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 19612867 | 16 | 282.6K | 43.3 | 175 | 1090 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 11315783 | 16 | 170.1K | 76.8 | 241 | 367 |
| ycsb-f | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2380082 | 16 | 38.6K | 31.4 | 79.9 | 53477 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2304 MiB | 0 | 408985 | 15.2 | 32.4 | 47.4 | 9395763 | 14.1 | 39.9 | 54.0 |
| ycsb-f | rocksdb | 1389 MiB | 0 | 362912 | 18.9 | 37.9 | 46.8 | 5292028 | 29.2 | 71.2 | 89.1 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 195543 | 8.83 | 19.1 | 47.1 | 994736 | 7.68 | 17.0 | 43.8 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 9808119 | 98.4 | 84.5 | 215 | 1548 |
| ycsb-f | pigeonhole | row read | 9804748 | 14.1 | 14.2 | 39.9 | 54.0 |
| ycsb-f | rocksdb | read-modify-write | 5660843 | 157 | 160 | 283 | 401 |
| ycsb-f | rocksdb | row read | 5654940 | 30.6 | 28.3 | 70.7 | 88.6 |
| ycsb-f | sqlite-eav | read-modify-write | 1189803 | 737 | 39.7 | 128 | 128975 |
| ycsb-f | sqlite-eav | row read | 1190279 | 7.83 | 7.93 | 17.3 | 44.5 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1009111 | 10.3% | 2 → 2 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1128898 | 11.5% | 5 → 4 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1082161 | 11.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 686051 | 7.0% | 2 → 1 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1329523 | 13.6% | 1 → 1 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1160334 | 11.8% | 3 → 2 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 975819 | 9.9% | 2 → 2 | 1 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 456349 | 4.7% | 0 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 237509 | 2.4% | 0 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 764011 | 7.8% | 0 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 978353 | 10.0% | 0 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 0 | 0.0 | 103 | 26 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.28 | 0.48 | 0.50 | 13098 (1225) | 0 |
