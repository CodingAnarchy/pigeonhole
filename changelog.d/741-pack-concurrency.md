### Changed
- Packing sizes the active shards by the commits in flight (D257): the mean number of client commits outstanding over the store (`round(C / 0.75)` shards at least) grows the active shards and gates a shrink, so one fast writer packs onto one shard (within seconds of a steady load, not after D255's 30 s window) while sixteen concurrent clients keep sixteen. The spin term D256 added to the busy measure is dropped (the spin counters and metrics stay). The stall and saturation growths and floors are unchanged.

### Added
- `Metrics::shard_commit_inflight_nanos`, `ShardStats::commit_inflight_nanos` and the bench's per-shard `commit_inflight_nanos`: the commits' time in flight from submit to reply; its rise over an interval, divided by the interval, is the mean number of commits in flight.
