**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 7.40 [non-reference (D5)]
Build: release, rev 3ee90c215206. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 79504040 | 16 | 1.20M | 8.57 | 44.5 | 85.0 |
| ycsb-a | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 51147889 | 16 | 696.4K | 16.3 | 68.6 | 95.7 |
| ycsb-a | sqlite-eav | page_cache=320MiB buffered | 1000000 | 5224758 | 16 | 85.0K | 10.8 | 1114 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2816 MiB | 0 | 458150 | 16.9 | 43.8 | 109 | 39292082 | 17.0 | 50.2 | 97.3 |
| ycsb-a | rocksdb | 1471 MiB | 0 | 451912 | 20.1 | 40.7 | 50.2 | 25120931 | 33.0 | 75.8 | 93.2 |
| ycsb-a | sqlite-eav | 1788 MiB | 0 | 280823 | 9.09 | 31.4 | 53.5 | 2330629 | 8.32 | 30.3 | 52.2 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 39753808 | 8.46 | 7.10 | 16.8 | 28.9 |
| ycsb-a | pigeonhole | row read | 39750232 | 17.1 | 17.0 | 50.2 | 97.3 |
| ycsb-a | rocksdb | put | 25575046 | 11.5 | 11.3 | 20.0 | 110 |
| ycsb-a | rocksdb | row read | 25572843 | 33.9 | 32.8 | 75.3 | 93.2 |
| ycsb-a | sqlite-eav | put | 2613306 | 356 | 12.5 | 4063 | 78643 |
| ycsb-a | sqlite-eav | row read | 2611452 | 9.22 | 8.38 | 30.5 | 52.5 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 5710774 | 14.4% | 1 → 2 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 4892487 | 12.3% | 3 → 8 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 4869677 | 12.2% | 2 → 2 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 5045888 | 12.7% | 4 → 4 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 4947331 | 12.4% | 1 → 1 | 1 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 4750429 | 11.9% | 1 → 2 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 4301733 | 10.8% | 2 → 2 | 1 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 4053727 | 10.2% | 1 → 0 | 0 | 0 | 4 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 559863 | 1.4% | 2 → 0 | 0 | 0 | 3 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 621899 | 1.6% | 2 → 0 | 0 | 0 | 2 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 2 | 25.1 | 430 | 109 | 14 (17) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.00 | 2556 (80) | 0 |
