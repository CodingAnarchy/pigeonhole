### Changed
- The L0 write stall starts at twice the compaction trigger (8 L0 files at the defaults) instead of at the trigger itself (D234). Writers are no longer paced while a compaction is merely running: a single shard wrote ~6% more on the reference machine, with no measurable read cost.
