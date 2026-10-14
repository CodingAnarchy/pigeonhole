### Added
- `phdb-bench adjacency-prefix` and `time-series-range`: adjacency's vertex scans filtered by an edge-qualifier prefix, and time-series scans of points 12 to 6 hours old (`BenchOp::ScanFiltered`), on every store. Not in `all`; the Phase 3 gate's `contended` step reports them (D247).
