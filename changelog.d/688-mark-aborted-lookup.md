### Fixed
- Application-owned stores with cross-shard batches no longer stall commits into `Busy` under a storm of refused two-phase rounds. Recording each aborted round scanned the shard's whole log of recent commits; it is now a lookup.
