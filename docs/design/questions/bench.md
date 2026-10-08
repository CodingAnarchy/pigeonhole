# Bench questions (Phase 2)

## Proposed decision: the `metric` family compacts FIFO in Pigeonhole and RocksDB; RocksDB scans merge column families only once `metric` is written (#236; D163 point 4)
D163 asked to switch Pigeonhole's `metric` family to `FifoByTime` once the picker existed, and to give RocksDB FIFO compaction with a TTL as the fair counterpart. RocksDB's FIFO is per column family, the runner kept every family in one, and `BenchOp::Scan` carries no family. So the question was how a scan finds `metric` cells without slowing the other workloads' scans, the sparse-wide gate's included.

The options weighed:
- **Route by key prefix** (`ts:` rows live only in `metric`). This is cheapest, but the runner's correctness would hang on the workload's key layout.
- **A per-run workload hook** opening the default column family as FIFO for `time-series-ttl`. This needs an ICR on `Runner`, and it would compact `metric`'s neighbours FIFO in a mixed run.
- **Merge both column families in every scan.** This is always correct, but adds a seek on an empty column family to every scan of every workload.
- **Merge only once `metric` was written** (chosen).

**Interim behavior:**
- Pigeonhole's `metric` family is `ttl(1 day)` plus `Compaction::FifoByTime`.
- RocksDB keeps `metric` cells in a `metric` column family with `DBCompactionStyle::Fifo`, `set_ttl(1 day)` and no size cap (`max_table_files_size = u64::MAX`, as Pigeonhole's FIFO has none). The other families stay in the default column family.
- RocksDB routes Put, PutAt, Get and GetRow by family. A scan merges the two column families' iterators in key order (keys never collide, since the family byte is part of the key), but only once the runner has written a `metric` cell. A run that never writes one, every workload but `time-series-ttl`, scans exactly as before.
- Caveat for comparisons: RocksDB's FIFO TTL counts from a file's creation, not from the cells' event times, so it drops the loaded back-dated points later than Pigeonhole does. Within one run (well under a day) it drops nothing. `docs/bench.md` says so next to the store-size note. SQLite and fjall have no FIFO and keep filtering on read.
- The agreement tests (RocksDB, SQLite and fjall against Pigeonhole on every workload) pass: every engine reads the same cells.
