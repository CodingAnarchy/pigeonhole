### Changed
- Tablet packing (D217) counts a shard as busy while its group fsyncs are in flight, not only while its thread works (D248). A durable (`GroupSync`/`Sync`) load bound by its fsyncs no longer packs onto too few shards. `Buffered` and `None` stores pack as before, and application-owned stores stay unpacked.

### Added
- `ShardStats::sync_busy_nanos`: the shard's group-sync in-flight time (D248).
