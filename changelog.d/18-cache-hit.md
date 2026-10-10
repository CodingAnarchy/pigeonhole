### Changed
- Block-cache hits scale across threads (#18). A hit takes its shard's lock shared and leaves a saturated hit count unwritten, and the block cache sizes its shards itself (up to 64) instead of using one per engine shard. 10 threads hitting the cache: 371 → 106 ns per hit. ycsb-c with 8 client threads on one engine shard: 863K → 1.67M ops/s.
