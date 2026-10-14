### Added
- `phdb-bench --max-secs S`: the longest measured phase; past it no client starts another operation, the run reports what it measured, and the record says `capped`. The Phase 3 gate's `contended` step bounds every run at 3× its minimum, so a slow row reports instead of stretching the window (D247).
