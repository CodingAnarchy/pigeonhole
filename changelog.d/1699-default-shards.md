### Added
- **`Options::default_shards(n)`** (ICR 0132): the shard count an open uses when `shards` is unset and the database has none of its own (a new file, or one without a change feed). A change feed's recorded count still wins, so it never reshards a feed.
