**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.16 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 31706826 | 16 | 528.0K | 14.1 | 118 | 173 |
| time-series-ttl | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 16293435 | 16 | 245.7K | 31.0 | 210 | 242 |
| time-series-ttl | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2465594 | 16 | 40.4K | 8.38 | 110 | 78643 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 3392 MiB | 0 | 5001122 | 3.07 | 7.39 | 21.0 | 1336156 | 3.12 | 7.93 | 21.5 |
| time-series-ttl | rocksdb | 926 MiB | 0 | 2583209 | 2.43 | 10.2 | 15.4 | 674674 | 2.50 | 10.6 | 15.2 |
| time-series-ttl | sqlite-eav | 401 MiB | 0 | 403908 | 5.70 | 16.8 | 38.1 | 88302 | 5.50 | 16.0 | 39.4 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | get | 6337278 | 3.40 | 3.07 | 7.49 | 21.0 |
| time-series-ttl | pigeonhole | put at | 12681584 | 61.1 | 53.2 | 142 | 262 |
| time-series-ttl | pigeonhole | scan | 12687964 | 12.3 | 11.6 | 24.6 | 42.2 |
| time-series-ttl | rocksdb | get | 3257883 | 3.25 | 2.45 | 10.3 | 15.4 |
| time-series-ttl | rocksdb | put at | 6517142 | 137 | 132 | 222 | 264 |
| time-series-ttl | rocksdb | scan | 6518410 | 24.3 | 22.4 | 57.9 | 81.9 |
| time-series-ttl | sqlite-eav | get | 492210 | 5.83 | 5.66 | 16.6 | 38.7 |
| time-series-ttl | sqlite-eav | put at | 986914 | 899 | 39.2 | 2998 | 179306 |
| time-series-ttl | sqlite-eav | scan | 986470 | 6.96 | 6.69 | 18.3 | 41.0 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 855846 | 6.7% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 615743 | 4.9% | 1 → 3 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 555768 | 4.4% | 1 → 16 | 0 | 8 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 607288 | 4.8% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 1110098 | 8.8% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 1657305 | 13.1% | 1 → 1 | 33 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 1015047 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 621601 | 4.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 642039 | 5.1% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 950914 | 7.5% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 805205 | 6.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 683866 | 5.4% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 631452 | 5.0% | 1 → 1 | 0 | 0 | 1 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 712617 | 5.6% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 702215 | 5.5% | 2 → 2 | 0 | 0 | 0 |
| time-series-ttl | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 514580 | 4.1% | 1 → 9 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-ttl | pigeonhole | 16 | 0 | 0.0 | 149 | 59 | 0 (0) | 0 | 0 | 1 | 0.1 | 0.06 | 0.36 | 0.40 | 84314 (11147) | 23 |
