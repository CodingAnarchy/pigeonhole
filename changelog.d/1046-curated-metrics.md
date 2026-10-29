### Changed
- **Breaking (before the 1.0 API freeze; ICR 0087):** `EngineMetrics`, `CrossShardMetrics` and `IoRings` keep the counters an application acts on (commits and latency, flushes and compactions, write stalls, the block cache, file growth, reader slots, threads, transactions, cross-shard rounds, rings). The engine's mechanism counters leave the public structs, among them shard parks and wakes, WAL unpin passes, visibility waits, cross-shard refusals by reason, share waits and ring pools.
- **Breaking:** `WalFile::checkpoint` and `end` are a `WalPosition { epoch, offset }` instead of tuples.

### Removed
- **Breaking:** `CrossShardMetrics::refused_below_floor` and `refused_move_raise`, always 0 since cross-shard commits pick their timestamp at their decision (D263), so no participant refuses a round for its timestamp.
