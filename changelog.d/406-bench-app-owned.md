### Added
- `phdb-bench app-owned`: application-owned mode on the default options, skewed writes inline from each shard's thread with no pre-split (the balancer and packing on) and about 20% of the commits batches across shards. The Phase 3 gate's `contended` step reports it at both durabilities (D247).
