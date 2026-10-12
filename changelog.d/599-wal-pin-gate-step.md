### Added
- The Phase 3 gate window measures the `wal_pin_bytes` default (#527): a `wal-pin` step runs a mixed-temperature workload at 2× and 4× the memtable budget, then reopens after an exit without close. The summary prints the write path (unpin passes, small flushes, the WAL's size on disk) and the replaying open's time for each.
