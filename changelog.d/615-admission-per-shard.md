### Changed
- Async commits are admitted on their own shard's count instead of one engine-wide count (D232, amends D210), so commits to different shards no longer share a contended cache line. The per-shard bound and arrival order are unchanged.
