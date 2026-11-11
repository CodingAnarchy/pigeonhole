### Added
- **`Options::default_shards(n)`** (ICR 0132): the shard count an open uses when `shards` is unset and the database has none of its own (a new file, or one without a change feed). A change feed's recorded count still wins, so it never reshards a feed.

### Fixed
- **A restarted `phdb serve` kept the library's shard count (one per physical core) instead of the server's default (half the physical cores).** The count isn't stored in the file, so every restart without `--shards` resharded. With a pinned driver on every core, the server's threads were then placed on the shards' SMT siblings. Serve now applies its default on every open, cluster partitions included.
