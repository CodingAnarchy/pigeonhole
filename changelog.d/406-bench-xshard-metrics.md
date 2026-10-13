### Changed
- `phdb-bench` records the engine's cross-shard two-phase counters (`Metrics::cross_shard`: rounds, refused rounds, refusals by reason) with each run's stall counters, and D246's wasted rounds per cross-shard commit now come from the refused rounds, with the sequence-number inference kept beside them as a cross-check (ICR 0043).
