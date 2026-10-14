### Added
- `phdb-bench --async N` (Pigeonhole): each client thread keeps N operations in flight through the async API (`commit_async`, `read_async`, `get_async`). The Phase 3 gate's `contended` step reports ycsb-a and ycsb-c with 16 in flight at both durabilities against the synchronous runs (D247).
