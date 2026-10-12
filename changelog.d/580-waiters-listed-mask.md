### Changed
- A durable or async commit's visibility publish no longer checks every shard's waiter list: the registry keeps a bit per shard that lists a waiter, so a publish with nobody waiting is one load, and a publish visits only the shards with waiters. About 30 instructions fewer per commit at one shard; at 32 shards, one load per publish instead of 32.
