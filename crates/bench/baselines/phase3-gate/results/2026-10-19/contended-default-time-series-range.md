**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 9.08 [non-reference (D5)]
Build: release, rev 5933e23e6f4f. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 2612250 | 16 | 19.5K | 76.8 | 7373 | 10945 |
| time-series-range | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 2879891 | 16 | 21.6K | 92.2 | 13435 | 20972 |
| time-series-range | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 1367185 | 16 | 12.9K | 78.8 | 18481 | 79692 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 512 MiB | 0 | 427376 | 4.05 | 12.3 | 22.1 | 94155 | 3.82 | 12.0 | 16.6 |
| time-series-range | rocksdb | 272 MiB | 0 | 470017 | 3.57 | 13.1 | 21.9 | 105183 | 3.23 | 11.1 | 14.4 |
| time-series-range | sqlite-eav | 320 MiB | 0 | 227157 | 12.9 | 281 | 430 | 45398 | 13.7 | 303 | 424 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | filtered scan | 1045358 | 1873 | 1286 | 9110 | 11731 |
| time-series-range | pigeonhole | get | 521531 | 4.62 | 4.00 | 12.2 | 21.2 |
| time-series-range | pigeonhole | put at | 1045361 | 176 | 64.8 | 2703 | 4112 |
| time-series-range | rocksdb | filtered scan | 1152310 | 1622 | 138 | 15925 | 23855 |
| time-series-range | rocksdb | get | 575200 | 4.22 | 3.49 | 12.5 | 21.2 |
| time-series-range | rocksdb | put at | 1152381 | 226 | 125 | 3097 | 4850 |
| time-series-range | sqlite-eav | filtered scan | 546999 | 1151 | 118 | 14418 | 22938 |
| time-series-range | sqlite-eav | get | 272555 | 25.9 | 13.1 | 285 | 428 |
| time-series-range | sqlite-eav | put at | 547631 | 1864 | 107 | 33817 | 128975 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 38366 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 39117 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 52624 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 101771 | 9.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 188123 | 18.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 30730 | 2.9% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 74829 | 7.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 64304 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 90020 | 8.6% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 38893 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 94238 | 9.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 92796 | 8.9% | 2 → 2 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 41578 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 45456 | 4.3% | 1 → 1 | 0 | 0 | 0 |
| time-series-range | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 52516 | 5.0% | 2 → 2 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| time-series-range | pigeonhole | 16 | 0 | 0.0 | 5 | 0 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.73 | 0.75 | 0.40 | 2011 (43) | 0 |
