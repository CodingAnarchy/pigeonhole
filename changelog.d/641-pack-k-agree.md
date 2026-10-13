### Fixed
- Under tablet packing, every shard now uses the same count of active shards: shard 0 decides it and the others follow. Before, each shard kept its own count, and the shards could disagree for good (one treating itself as active while the others packed), so tablets were placed on fewer or more shards than the load called for (#641).
