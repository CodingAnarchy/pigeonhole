### Added
- `shard_stats()` reports each shard's `busy_nanos`, and `engine_metrics()` their sum (`shard_busy_nanos`): the time an engine-owned shard thread spent handling work, not spinning or parked (D217, ICR 0033). Over an interval, it shows how busy each shard was. `phdb-bench` records it per shard for the measured phase. `pigeonhole-runtime` adds `Submitter::busy_nanos`.
