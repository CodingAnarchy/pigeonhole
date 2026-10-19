**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.82 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 31913311 | 16 | 531.9K | 12.4 | 117 | 174 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 17579158 | 16 | 245.6K | 33.0 | 206 | 238 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2562704 | 16 | 40.7K | 8.38 | 108 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 3136 MiB | 0 | 5033435 | 2.78 | 7.13 | 19.7 | 1344990 | 2.83 | 7.74 | 20.4 |
| time-series-ttl | rocksdb | 991 MiB | 0 | 2784921 | 2.56 | 10.9 | 15.7 | 729658 | 2.65 | 11.1 | 15.8 |
| time-series-ttl | sqlite-eav | 407 MiB | 0 | 419427 | 5.70 | 16.5 | 36.1 | 92172 | 5.50 | 16.3 | 39.4 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 6378425 | 3.10 | 2.80 | 7.26 | 19.8 |
| time-series-ttl | pigeonhole | put at | 12764223 | 62.5 | 53.8 | 143 | 293 |
| time-series-ttl | pigeonhole | scan | 12770663 | 10.6 | 9.98 | 22.0 | 38.9 |
| time-series-ttl | rocksdb | get | 3514579 | 3.39 | 2.58 | 10.9 | 15.7 |
| time-series-ttl | rocksdb | put at | 7031712 | 135 | 131 | 218 | 266 |
| time-series-ttl | rocksdb | scan | 7032867 | 25.8 | 23.7 | 65.3 | 94.7 |
| time-series-ttl | sqlite-eav | get | 511599 | 5.81 | 5.66 | 16.5 | 36.6 |
| time-series-ttl | sqlite-eav | put at | 1025625 | 898 | 39.2 | 2474 | 179306 |
| time-series-ttl | sqlite-eav | scan | 1025480 | 6.94 | 6.69 | 18.3 | 41.5 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 756166 | 5.9% | 1 → 3 | 0 | 12 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 962134 | 7.5% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 861923 | 6.8% | 1 → 5 | 0 | 6 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 739434 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 724679 | 5.7% | 2 → 3 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 822580 | 6.4% | 1 → 1 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 935512 | 7.3% | 2 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 1510350 | 11.8% | 1 → 1 | 38 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 686778 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 658745 | 5.2% | 1 → 16 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 1042236 | 8.2% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 723845 | 5.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 977526 | 7.7% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 979624 | 7.7% | 2 → 2 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 209741 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 172950 | 1.4% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 0 | 0.0 | 161 | 73 | 2 (14) | 0 | 0 | 0 | 0.0 | 0.29 | 0.47 | 0.40 | 77493 (10675) | 17 |
