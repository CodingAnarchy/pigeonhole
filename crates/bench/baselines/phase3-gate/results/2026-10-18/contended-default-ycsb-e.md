**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 8.19 [non-reference (D5)]
Build: release, rev f61510ddabbe. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1000000 | 2000406 | 16 | 30.5K | 101 | 1729 | 2425 |
| ycsb-e | rocksdb | write_buffer=64MiB cache=256MiB bloom=10 sync load=buffered | 1000000 | 6554434 | 16 | 108.3K | 129 | 367 | 2179 |
| ycsb-e | sqlite-eav | page_cache=320MiB sync load=buffered | 1000000 | 2814054 | 16 | 41.7K | 183 | 3441 | 34079 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | put | 99894 | 118 | 92.7 | 332 | 1114 |
| ycsb-e | pigeonhole | scan | 1900512 | 544 | 105 | 1729 | 2425 |
| ycsb-e | rocksdb | put | 327780 | 290 | 128 | 2884 | 16777 |
| ycsb-e | rocksdb | scan | 6226654 | 138 | 129 | 350 | 408 |
| ycsb-e | sqlite-eav | put | 140345 | 3735 | 741 | 54002 | 130023 |
| ycsb-e | sqlite-eav | scan | 2673709 | 202 | 174 | 631 | 856 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 0 | 6500 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 1 | 6222 | 6.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 2 | 5029 | 5.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 3 | 11059 | 11.1% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 4 | 9019 | 9.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 5 | 6543 | 6.5% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 6 | 3731 | 3.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 7 | 8281 | 8.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 8 | 3847 | 3.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 9 | 3986 | 4.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 10 | 3550 | 3.6% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 11 | 8414 | 8.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 12 | 5863 | 5.9% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 13 | 2296 | 2.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 14 | 11142 | 11.2% | 1 → 1 | 0 | 0 | 0 |
| ycsb-e | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on | 15 | 4412 | 4.4% | 1 → 1 | 0 | 0 | 0 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-e | pigeonhole | 16 | 0 | 0.0 | 3 | 1 | 0 (0) | 0 | 0 | 0 | 0.0 | 0.10 | 0.10 | 0.05 | 25 (0) | 0 |
