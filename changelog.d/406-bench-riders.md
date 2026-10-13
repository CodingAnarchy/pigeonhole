### Added
- `phdb-bench --riders` (Pigeonhole): snapshot and durable-snapshot readers, then a durable backup and a full compaction, beside the measured phase, reported in the record's `riders`; every record now has `close_ms`. The Phase 3 gate's `contended` step reports ycsb-a with riders at both durabilities (D247).
