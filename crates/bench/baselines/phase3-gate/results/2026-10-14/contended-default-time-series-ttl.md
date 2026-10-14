**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.33 [non-reference (D5)]
Build: release, rev 175c0ffc7318. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 29556676 | 16 | 489.4K | 11.5 | 124 | 954 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 15851479 | 16 | 243.6K | 30.3 | 213 | 244 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2460850 | 16 | 40.4K | 8.38 | 116 | 78643 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 2432 MiB | 0 | 4663623 | 2.64 | 6.53 | 17.0 | 1243439 | 2.67 | 6.82 | 17.3 |
| time-series-ttl | rocksdb | 909 MiB | 0 | 2513342 | 2.48 | 10.0 | 15.1 | 655813 | 2.53 | 10.2 | 14.9 |
| time-series-ttl | sqlite-eav | 401 MiB | 0 | 403179 | 5.70 | 16.6 | 37.6 | 88092 | 5.50 | 16.6 | 38.1 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 5907062 | 2.92 | 2.64 | 6.59 | 17.0 |
| time-series-ttl | pigeonhole | put at | 11821816 | 70.0 | 58.9 | 161 | 1475 |
| time-series-ttl | pigeonhole | scan | 11827798 | 9.85 | 9.47 | 18.4 | 34.0 |
| time-series-ttl | rocksdb | get | 3169155 | 3.25 | 2.48 | 10.1 | 15.0 |
| time-series-ttl | rocksdb | put at | 6340302 | 138 | 133 | 225 | 266 |
| time-series-ttl | rocksdb | scan | 6342022 | 23.9 | 22.0 | 56.6 | 79.9 |
| time-series-ttl | sqlite-eav | get | 491271 | 5.85 | 5.66 | 16.6 | 37.6 |
| time-series-ttl | sqlite-eav | put at | 985066 | 908 | 39.2 | 3195 | 179306 |
| time-series-ttl | sqlite-eav | scan | 984513 | 6.94 | 6.66 | 18.4 | 42.5 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 1378080 | 11.7% | 4 → 3 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 1288926 | 10.9% | 9 → 6 | 1 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 1018305 | 8.6% | 5 → 3 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 1094460 | 9.3% | 5 → 4 | 0 | 0 | 2 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1021274 | 8.6% | 4 → 2 | 0 | 0 | 3 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 894886 | 7.6% | 3 → 5 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 1125423 | 9.5% | 0 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 1350953 | 11.4% | 0 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 1300392 | 11.0% | 0 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 1349117 | 11.4% | 0 → 3 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 4 | 181.8 | 123 | 44 | 9 (17) | 0 | 0 | 0 | 0.0 | 0.17 | 0.39 | 0.40 | 37616 (4445) | 0 |
