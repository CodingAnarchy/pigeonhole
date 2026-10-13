### Added
- `phdb-bench sparse-wide-deletes`: sparse-wide's mix with 5% column and row deletes (`BenchOp::DeleteColumn`, `BenchOp::DeleteRow`), on every store. Not in `all`; the Phase 3 gate's `contended` step reports it (D247).
