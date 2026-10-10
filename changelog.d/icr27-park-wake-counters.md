### Added
- `pigeonhole-engine`: `Metrics::shard_idle` (idle parks of shard threads, and wakes of a parked shard) and `Metrics::commit_parks` (blocking commit waits that parked), ICR 0027. `pigeonhole-runtime`: `Submitter::idle_counts`. `phdb-bench` reports them per operation, and `--shard-spin US` sets `Options::shard_spin`.
