### Added
- `Pigeonhole::engine_metrics()` and `EngineMetrics` are documented public API (ICR 0044). Every field is named and documented (unit, counter or gauge; counters start at 0 at open and never reset). New: `EngineMetrics::commits(Durability) -> CommitStats` (count and p50/p99/p99.9 latency) and `cross_shard: CrossShardMetrics` (two-phase rounds and refusals by reason, ICR 0043).

### Changed
- `EngineMetrics` was a hidden bench hook that re-exported the engine's `Metrics`. It is now the public crate's own struct, and its fields are renamed from tuples to named fields (see ICR 0044 for the list). For example, `stalls.0`/`.1` became `write_stalls`/`write_stall_nanos`, `block_cache.0`/`.1` became `block_cache_hits`/`block_cache_misses`, and `pinned_threads` became `threads_started`/`threads_pinned`.
