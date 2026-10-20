**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.83 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 32726586 | 16 | 545.2K | 13.1 | 115 | 170 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 17051760 | 16 | 244.6K | 32.0 | 210 | 242 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2502955 | 16 | 40.5K | 8.45 | 113 | 78643 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 2752 MiB | 0 | 5161979 | 2.96 | 7.52 | 20.2 | 1380184 | 3.01 | 8.38 | 19.8 |
| time-series-ttl | rocksdb | 964 MiB | 0 | 2702225 | 2.50 | 10.5 | 15.6 | 707033 | 2.58 | 10.8 | 15.5 |
| time-series-ttl | sqlite-eav | 405 MiB | 0 | 409946 | 5.76 | 16.9 | 36.9 | 89767 | 5.57 | 16.6 | 36.4 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 6542163 | 3.32 | 2.96 | 7.65 | 20.2 |
| time-series-ttl | pigeonhole | put at | 13088744 | 59.6 | 52.0 | 140 | 247 |
| time-series-ttl | pigeonhole | scan | 13095679 | 11.4 | 10.9 | 22.4 | 42.0 |
| time-series-ttl | rocksdb | get | 3409258 | 3.32 | 2.51 | 10.6 | 15.5 |
| time-series-ttl | rocksdb | put at | 6821365 | 136 | 131 | 222 | 266 |
| time-series-ttl | rocksdb | scan | 6821137 | 25.1 | 23.0 | 62.5 | 90.1 |
| time-series-ttl | sqlite-eav | get | 499713 | 5.89 | 5.73 | 16.8 | 36.9 |
| time-series-ttl | sqlite-eav | put at | 1001793 | 902 | 39.2 | 3195 | 179306 |
| time-series-ttl | sqlite-eav | scan | 1001449 | 6.98 | 6.72 | 18.4 | 41.2 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 831735 | 6.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 718217 | 5.5% | 2 → 2 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 390928 | 3.0% | 1 → 16 | 0 | 5 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 828408 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 951718 | 7.3% | 1 → 1 | 1 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 913272 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 1350333 | 10.3% | 1 → 1 | 20 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 976962 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 724655 | 5.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 671346 | 5.1% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 786718 | 6.0% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 672423 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 972240 | 7.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 835242 | 6.4% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 730637 | 5.6% | 1 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 733910 | 5.6% | 1 → 0 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 0 | 0.0 | 146 | 50 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.05 | 0.36 | 0.40 | 87873 (12940) | 31 |
