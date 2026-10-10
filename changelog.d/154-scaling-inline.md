### Changed
- `phdb-bench scaling` measures the thread-per-core gate as D204 defines it: application-owned shards, each thread committing the rows its shard owns inline with 16 commits in flight, at 1 shard and at N up to the core count. It then reports, without gating, the engine-owned shape: synchronous clients, 4 per shard, at N/2 shards. The JSON gains `scaling_sync`. A `scaling.json` from before this is not a p99 baseline.

### Added
- `Table::shard_of(row)` (and `Engine::shard_of`): the shard that owns a row now. It's a routing hint for application-owned mode, so a shard thread can commit the rows it owns with no handoff. It may change after a split or move, and commits stay correct whatever it says (ICR 0022).
