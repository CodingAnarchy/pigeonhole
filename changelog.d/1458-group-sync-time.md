### Added
- `EngineMetrics::group_sync_nanos`: the time group syncs were in flight, summed over shards. Over `group_syncs`, it gives the mean group sync time, the WAL device's sync latency as durable commits see it. `pigeonhole-otel` exports it as `pigeonhole.wal.group_sync.time` (D316 A3, ICR 0116).
