### Added
- `EngineMetrics::memtable_bytes` and `memtable_limit_bytes`: the memtable arenas' bytes in use and their size, summed over shards. `pigeonhole-otel` exports them as `pigeonhole.memtable.size` and `pigeonhole.memtable.limit`, the early warning before commits wait for room (D316 A2, ICR 0116).
