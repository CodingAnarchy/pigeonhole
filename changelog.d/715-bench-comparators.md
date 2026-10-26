### Added
- `phdb-bench app-owned --engine-owned`: the application-owned row's workload (the same stream, ~20% cross-shard batches, 16 commits in flight per shard's thread) on engine-owned shards, its D280 comparator (#816, #710).
- `phdb-bench --riders` on RocksDB: a `Checkpoint` and then a `BackupEngine` backup 2 s into the measured phase, timed, with each side's backup bytes, beside Pigeonhole's durable backup (D280, #715). The gate's contended step runs both.
