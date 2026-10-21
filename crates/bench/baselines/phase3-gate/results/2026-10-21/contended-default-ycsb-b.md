**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.17 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 86201691 | 16 | 1.29M | 4.35 | 76.3 | 114 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 37852244 | 16 | 436.5K | 27.4 | 179 | 717 |
| ycsb-b | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 10173142 | 16 | 166.8K | 7.55 | 1122 | 18350 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 2176 MiB | 0 | 459051 | 14.6 | 34.3 | 50.9 | 81429099 | 3.79 | 36.6 | 51.7 |
| ycsb-b | rocksdb | 1274 MiB | 0 | 456055 | 16.2 | 32.1 | 42.5 | 35500263 | 26.6 | 69.1 | 87.6 |
| ycsb-b | sqlite-eav | 1788 MiB | 0 | 406517 | 7.97 | 42.2 | 80.4 | 9257076 | 7.39 | 39.7 | 72.7 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | put | 4313541 | 63.2 | 56.6 | 128 | 236 |
| ycsb-b | pigeonhole | row read | 81888150 | 8.85 | 3.84 | 36.6 | 51.7 |
| ycsb-b | rocksdb | put | 1895926 | 174 | 144 | 999 | 2073 |
| ycsb-b | rocksdb | row read | 35956318 | 29.2 | 26.5 | 69.1 | 87.6 |
| ycsb-b | sqlite-eav | put | 509549 | 1709 | 52.0 | 33423 | 103809 |
| ycsb-b | sqlite-eav | row read | 9663593 | 9.26 | 7.42 | 39.9 | 72.7 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 915662 | 21.2% | 2 → 3 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 706926 | 16.4% | 3 → 3 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 947072 | 22.0% | 2 → 3 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 788537 | 18.3% | 1 → 2 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 655947 | 15.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 205714 | 4.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 27550 | 0.6% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 66133 | 1.5% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 16 | 0 | 0.0 | 48 | 11 | 1 (1) | 0 | 0 | 0 | 0.0 | 0.01 | 0.05 | 0.05 | 2906 (123) | 0 |
