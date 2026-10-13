### Added
- The Phase 3 gate's `contended` step reports ycsb-a with 16 KiB values (blob separation) at both durabilities against RocksDB and SQLite, flagged when it trails RocksDB by more than 3× what the judged ycsb-a does (D247).
