# 0024: `BlockCache::hits_and_misses`

**Status:** Approved (coordinator, 2026-10-10; #57).

## Change

`pigeonhole-cache`, additive:

```rust
impl BlockCache {
    /// Lookups (`get`) that found their block, and lookups that did not, since the cache was
    /// made. A disabled cache counts nothing.
    pub fn hits_and_misses(&self) -> (u64, u64);
}
```

## Why

`Engine::metrics().block_cache` (engine Milestone B, #37) has reported `(hits, misses)` since Phase 1, but the cache kept no counters, so the pair was always `(0, 0)`. The Phase 3 latency work sizes the block cache from its hit rate, and the cold-get gate step (#87) reports block-cache misses per get beside the reads it counts at the VFS.

## Semantics

- **Counted in `get` only.** A lookup that finds its block is a hit; one that does not is a miss (the caller then reads the block and usually inserts it). `insert`, `erase_file(s)` and eviction count nothing.
- **Per shard, relaxed.** Two `AtomicU64` per cache shard, in the shard's existing 128-byte-padded slot and placed before its lock, so they share the lock word's 64-byte line: a lookup adds to a line its read lock just wrote, and shards never share a counter line. The accessor sums the shards; a reading taken during lookups may lag them, never count one twice.
- **A disabled cache** (no shards) counts nothing.
- **The row cache** is unchanged (it has its own `row_cache_stats`, #446).

## Callers

- `pigeonhole-engine`: `Engine::metrics` fills `Metrics::block_cache` from it.
- `pigeonhole`: no change (`engine_metrics()` passes the engine's metrics through).
