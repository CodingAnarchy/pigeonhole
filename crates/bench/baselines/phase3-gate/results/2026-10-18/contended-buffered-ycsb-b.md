**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 1.62 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 99816034 | 16 | 1.58M | 5.18 | 36.4 | 51.7 |
| ycsb-b | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 55376351 | 16 | 612.9K | 25.1 | 52.2 | 69.1 |
| ycsb-b | sqlite-eav | page_cache=320MiB buffered | 1000000 | 13956225 | 16 | 222.4K | 14.3 | 1106 | 8290 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 1984 MiB | 0 | 459090 | 14.8 | 36.9 | 66.6 | 94361163 | 4.54 | 36.6 | 50.7 |
| ycsb-b | rocksdb | 1330 MiB | 0 | 458403 | 14.7 | 28.4 | 36.4 | 52146022 | 26.2 | 52.5 | 69.1 |
| ycsb-b | sqlite-eav | 1788 MiB | 0 | 426289 | 15.3 | 112 | 178 | 12830363 | 13.6 | 97.3 | 151 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | put | 4995781 | 12.5 | 9.92 | 22.4 | 148 |
| ycsb-b | pigeonhole | row read | 94820253 | 9.09 | 4.61 | 36.6 | 50.7 |
| ycsb-b | rocksdb | put | 2771926 | 6.76 | 6.33 | 13.9 | 19.6 |
| ycsb-b | rocksdb | row read | 52604425 | 26.7 | 26.1 | 52.5 | 69.1 |
| ycsb-b | sqlite-eav | put | 699573 | 1002 | 60.2 | 18350 | 53740 |
| ycsb-b | sqlite-eav | row read | 13256652 | 22.2 | 13.6 | 97.8 | 152 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1293974 | 25.9% | 1 → 4 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 1387679 | 27.8% | 0 → 6 | 0 | 0 | 6 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 1325844 | 26.5% | 1 → 4 | 0 | 0 | 0 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 106359 | 2.1% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 46134 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 157609 | 3.2% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 78454 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 49127 | 1.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 84949 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 66317 | 1.3% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 90557 | 1.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 80944 | 1.6% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 38752 | 0.8% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 86712 | 1.7% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 102370 | 2.0% | 1 → 0 | 0 | 0 | 1 |
| ycsb-b | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-b | pigeonhole | 16 | 3 | 135.0 | 61 | 20 | 2 (6) | 0 | 0 | 0 | 0.0 | 0.02 | 0.02 | 0.00 | 827 (18) | 0 |
