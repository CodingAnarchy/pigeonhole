**Environment:** AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 125 GiB), linux 6.8.0-139-generic x86_64, xfs, load 13.45 [non-reference (D5)]
Build: release, rev 516b59b8f230. Not reference hardware: reported, never gates a phase (D5).

| Workload | Store | Settings | Records | Ops | Threads | Ops/s | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 1000000 | 31398286 | 16 | 546.6K | 225 | 1245 | 2425 |

| Workload | Store | Store size | Busy retries | Cold gets | Cold p50 µs | Cold p99 µs | Cold p99.9 µs | Hot gets | Hot p50 µs | Hot p99 µs | Hot p99.9 µs |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 2432 MiB | 0 | 436185 | 295 | 1147 | 1688 | 15260059 | 246 | 1434 | 2245 |

| Workload | Store | Op | Count | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---|---|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | put | 15702042 | 272 | 209 | 905 | 5145 |
| ycsb-a | pigeonhole | row read | 15696244 | 329 | 248 | 1425 | 2228 |

| Workload | Store | Settings | Shard | Commits | Share | Tablets (start → end) | Splits | Merges | Moves |
|---|---|---|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 0 | 1220997 | 7.8% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 1 | 1095490 | 7.0% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 2 | 1066689 | 6.8% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 3 | 1184969 | 7.5% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 4 | 1113903 | 7.1% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 5 | 889229 | 5.7% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 6 | 955568 | 6.1% | 4 → 4 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 7 | 850531 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 8 | 691852 | 4.4% | 1 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 9 | 705184 | 4.5% | 2 → 2 | 0 | 0 | 1 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 10 | 855442 | 5.4% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 11 | 995040 | 6.3% | 2 → 2 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 12 | 1052923 | 6.7% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 13 | 1161454 | 7.4% | 1 → 2 | 1 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 14 | 826937 | 5.3% | 1 → 1 | 0 | 0 | 0 |
| ycsb-a | pigeonhole | shards=default(16) memtable=64MiB cache=256MiB bloom=10 group-sync(default) load=buffered tablets=on async=16 | 15 | 1035834 | 6.6% | 2 → 1 | 0 | 0 | 1 |

| Workload | Store | Threads | Write stalls | Stalled ms | Flushes | Compactions | Unpin passes (small flushes) | Inline WAL syncs | WAL rollover blocks | File growths | Growth ms | Shard parks / op | Shard wakes / op | Commit parks / op | Visibility waits (contended) | Slot refusals |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| ycsb-a | pigeonhole | 16 | 6 | 50.2 | 168 | 40 | 2 (3) | 0 | 0 | 0 | 0.0 | 0.04 | 0.36 | 0.00 | 69008 (7788) | 0 |
