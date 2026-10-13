### Fixed
- A table loaded faster than it compacts now splits and spreads over shards. Before, the balancer skipped the split whenever the tablet was compacting, which under a bulk load was always, so the whole load stayed on one shard (#636).
- A cross-shard commit that keeps meeting tablet changes, or a participant's timestamp floor, now fails with `Busy` only after the write-stall timeout. Before, it gave up after 16 refusals, often within seconds (D241, #636).

### Changed
- The `Busy` error message names tablet changes as a cause, along with flushes and compaction, and refers to `Options::write_stall_timeout` instead of a fixed 30 s.
