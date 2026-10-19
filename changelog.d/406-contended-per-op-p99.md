### Changed
- The Phase 3 gate's summary reports each contended workload's p99 per operation type against the same type in RocksDB and SQLite (read-modify-write against read-modify-write, read against read), beside the judged p99 over every operation, which is unchanged (D261).
