### Added
- `phdb-bench counters`: 16 clients, 90% increments of Zipfian counters and 10% reads; Pigeonhole's counter family against RocksDB's merge operator (an associative `i64` add), SQLite's upsert and fjall's locked read-modify-write. Not in `all`; the Phase 3 gate's `contended` step judges it (D247).
