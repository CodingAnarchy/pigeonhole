### Added
- Reader processes can follow the change feed (D310, #1248): `ReadTable::feed()` gives the same `Feed` a writer's `Table::feed()` does, through the writer's shared memory. The writer does no per-commit work for it. Across a writer restart the feed resumes from its own token on the new writer: a durable feed continues, and a visible one that delivered what a power loss took expires. `FeedBuilder::read_table` adds more tables. `from_oldest` isn't supported in a reader process (start from a snapshot).
### Changed
- The shared-memory layout is now version 8, which adds each shard's change-feed floor and checkpoint for reader processes. Writer and reader processes must run the same release.
