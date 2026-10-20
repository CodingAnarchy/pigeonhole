**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 4.39 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1000000 | 43461221 | 16 | 550.7K | 26.5 | 84.5 | 127 |
| ycsb-f | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 buffered | 1000000 | 46806868 | 16 | 646.0K | 21.5 | 66.6 | 91.6 |
| ycsb-f | sqlite-eav | page_cache=320MiB buffered | 1000000 | 4911707 | 16 | 81.8K | 11.1 | 1106 | 33423 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 2624 MiB | 0 | 447899 | 18.2 | 41.7 | 101 | 21279482 | 18.4 | 56.3 | 80.9 |
| ycsb-f | rocksdb | 1401 MiB | 0 | 449918 | 19.6 | 39.4 | 49.2 | 22948603 | 32.5 | 74.8 | 92.2 |
| ycsb-f | sqlite-eav | 1788 MiB | 0 | 274199 | 9.09 | 31.7 | 57.6 | 2179871 | 8.32 | 31.0 | 54.3 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | read-modify-write | 21733840 | 38.5 | 34.3 | 94.7 | 144 |
| ycsb-f | pigeonhole | row read | 21727381 | 18.9 | 18.4 | 56.3 | 81.4 |
| ycsb-f | rocksdb | read-modify-write | 23408347 | 15.6 | 13.0 | 38.7 | 91.1 |
| ycsb-f | rocksdb | row read | 23398521 | 33.3 | 32.3 | 74.8 | 91.6 |
| ycsb-f | sqlite-eav | read-modify-write | 2457637 | 372 | 13.3 | 3228 | 78643 |
| ycsb-f | sqlite-eav | row read | 2454070 | 9.11 | 8.45 | 31.0 | 54.5 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 0 | 1899986 | 8.7% | 2 → 2 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 1 | 2088726 | 9.6% | 2 → 4 | 1 | 0 | 4 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 2 | 2062308 | 9.5% | 2 → 5 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 3 | 2139136 | 9.8% | 2 → 3 | 1 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 4 | 2226748 | 10.2% | 1 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 5 | 1991159 | 9.2% | 2 → 2 | 2 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 6 | 2226143 | 10.2% | 1 → 2 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 7 | 1966217 | 9.0% | 2 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 8 | 2229584 | 10.3% | 2 → 3 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 9 | 2117259 | 9.7% | 1 → 2 | 0 | 0 | 3 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 10 | 193162 | 0.9% | 1 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 11 | 211708 | 1.0% | 1 → 0 | 2 | 0 | 3 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 12 | 190966 | 0.9% | 1 → 0 | 0 | 0 | 1 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 13 | 190738 | 0.9% | 2 → 0 | 0 | 0 | 2 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 14 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |
| ycsb-f | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 buffered tablets=on | 15 | 0 | 0.0% | 0 → 0 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-f | pigeonhole | 16 | 0 | 0.0 | 242 | 67 | 12 (15) | 0 | 0 | 0 | 0.0 | 0.00 | 0.01 | 0.50 | 18469 (1882) | 0 |
