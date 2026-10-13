### Changed
- `phdb-bench`'s ycsb-f read-modify-write is atomic across client threads in every store, so ycsb-f now runs at any `--threads`: Pigeonhole uses `commit_if` on the value it read and retries a lost race, RocksDB and fjall hold a row lock from the read to the write, SQLite uses an immediate transaction. The gate's `contended` step judges it at 16 threads (D247).
