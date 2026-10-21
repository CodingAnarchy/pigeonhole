**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 3.65 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 21672522 | 16 | 313.7K | 44.0 | 151 | 203 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 11944894 | 16 | 171.2K | 76.3 | 240 | 362 |
| ycsb-f | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2445387 | 16 | 38.6K | 31.0 | 80.9 | 53740 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2368 MiB | 0 | 415614 | 17.7 | 36.1 | 64.0 | 10418120 | 16.1 | 44.0 | 62.7 |
| ycsb-f | rocksdb | 1420 MiB | 0 | 367991 | 19.1 | 38.1 | 47.1 | 5601981 | 29.6 | 72.7 | 91.1 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 198331 | 8.83 | 19.7 | 49.9 | 1024660 | 7.78 | 17.7 | 48.1 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 10838788 | 85.4 | 79.4 | 164 | 237 |
| ycsb-f | pigeonhole | row read | 10833734 | 15.7 | 16.2 | 44.0 | 63.0 |
| ycsb-f | rocksdb | read-modify-write | 5974922 | 155 | 158 | 281 | 397 |
| ycsb-f | rocksdb | row read | 5969972 | 31.1 | 28.7 | 72.2 | 90.6 |
| ycsb-f | sqlite-eav | read-modify-write | 1222396 | 775 | 39.9 | 170 | 128975 |
| ycsb-f | sqlite-eav | row read | 1222991 | 7.92 | 8.00 | 18.0 | 48.4 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 866553 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 824123 | 7.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 500960 | 4.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 666626 | 6.2% | 3 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 554858 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 568592 | 5.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 1082144 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 630526 | 5.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 795847 | 7.3% | 1 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 548043 | 5.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 1082429 | 10.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 773235 | 7.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 725153 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 868433 | 8.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 351266 | 3.2% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 0 | 0.0 | 115 | 29 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.12 | 0.44 | 0.50 | 10959 (1248) | 0 |
