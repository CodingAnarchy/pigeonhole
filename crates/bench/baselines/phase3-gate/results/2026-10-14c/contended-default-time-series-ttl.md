**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.04 [non-reference (D5)]
Build: release, rev 234c5c2ff108. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 32232839 | 16 | 533.0K | 11.8 | 116 | 171 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 17618256 | 16 | 244.1K | 33.3 | 208 | 240 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2507935 | 16 | 39.8K | 8.45 | 124 | 78643 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 2752 MiB | 0 | 5084179 | 2.64 | 6.88 | 18.2 | 1358699 | 2.69 | 7.33 | 18.0 |
| time-series-ttl | rocksdb | 996 MiB | 0 | 2791053 | 2.54 | 10.9 | 15.9 | 731335 | 2.62 | 11.2 | 15.8 |
| time-series-ttl | sqlite-eav | 404 MiB | 0 | 410748 | 5.73 | 16.8 | 38.9 | 89977 | 5.57 | 16.2 | 37.1 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 6442878 | 2.96 | 2.65 | 6.94 | 18.2 |
| time-series-ttl | pigeonhole | put at | 12891645 | 63.0 | 55.6 | 141 | 201 |
| time-series-ttl | pigeonhole | scan | 12898316 | 10.1 | 9.54 | 20.6 | 35.1 |
| time-series-ttl | rocksdb | get | 3522388 | 3.38 | 2.56 | 10.9 | 15.8 |
| time-series-ttl | rocksdb | put at | 7047301 | 136 | 131 | 219 | 268 |
| time-series-ttl | rocksdb | scan | 7048567 | 26.0 | 23.8 | 66.6 | 96.3 |
| time-series-ttl | sqlite-eav | get | 500725 | 5.87 | 5.70 | 16.6 | 38.4 |
| time-series-ttl | sqlite-eav | put at | 1003743 | 912 | 39.4 | 3195 | 179306 |
| time-series-ttl | sqlite-eav | scan | 1003467 | 6.99 | 6.75 | 18.4 | 42.0 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 927186 | 7.2% | 3 → 3 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1030777 | 8.0% | 4 → 3 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1167217 | 9.1% | 3 → 2 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 642391 | 5.0% | 3 → 15 | 6 | 11 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1037687 | 8.0% | 3 → 2 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1110330 | 8.6% | 2 → 2 | 4 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 1115545 | 8.7% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 1123009 | 8.7% | 0 → 7 | 26 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 994704 | 7.7% | 0 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 795074 | 6.2% | 0 → 2 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 1112079 | 8.6% | 0 → 2 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 815903 | 6.3% | 0 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 1019743 | 7.9% | 0 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 0 | 0.0 | 164 | 71 | 2 (3) | 0 | 0 | 0 | 0.0 | 0.23 | 0.43 | 0.40 | 46250 (6009) | 16 |
