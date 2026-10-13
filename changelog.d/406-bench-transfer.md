### Added
- `phdb-bench transfer`: 16 clients moving one unit between two Zipfian accounts in a transaction that reads both; Pigeonhole's optimistic `Transaction` retries a conflict and the record counts them (`txn_conflicts`). Not in `all`; the Phase 3 gate's `contended` step reports it (D247).
