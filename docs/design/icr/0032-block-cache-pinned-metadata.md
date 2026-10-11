# 0032: pinned SST metadata in the block cache, and `Metrics` fields for it

**Status:** Approved (coordinator, 2026-10-11; D215, #87).

## Change

`pigeonhole-cache`, additive:

```rust
impl BlockCache {
    /// Counts `bytes` of blocks a reader keeps pinned while it is open, until a matching
    /// `sub_pinned`. A rise past half the capacity counts a warning, at most once until the
    /// pinned bytes fall below 40% of the capacity again.
    pub fn add_pinned(&self, bytes: usize);
    /// Ends the count of `bytes` an `add_pinned` began.
    pub fn sub_pinned(&self, bytes: usize);
    /// Bytes counted and not yet ended.
    pub fn pinned(&self) -> usize;
    /// Rises of the pinned bytes past half the capacity.
    pub fn pinned_warnings(&self) -> u64;
}

impl BlockHandle {
    /// Bytes the block counts against a cache's capacity.
    pub fn charge(&self) -> usize;
}
```

`pigeonhole-engine`, additive fields of `Metrics` (`#[non_exhaustive]`):

```rust
pub block_cache_pinned: u64,           // bytes of SST top indexes and filters pinned by open readers
pub block_cache_capacity: u64,         // EngineOptions::block_cache_bytes
pub block_cache_pinned_warnings: u64,  // rises of block_cache_pinned past half the capacity
```

`pigeonhole-sst`: no public change. An `SstReader` counts its top index and filters (`BlockHandle::charge` of each) in its cache's pinned bytes when it opens, and ends the count when it drops.

## Why

D215. An open SST pins its top index and both filters in the block cache, counted against its capacity and never evicted. Once they take most of the cache, the index partitions and data blocks reads come back for no longer stay cached: the first Phase 3 gate run measured 2.09 reads per cold get with a 64 MiB cache under ~185 MiB of pinned metadata. Nothing reported this. The owner chose metrics only, with no output from the library (a log or event sink is a candidate for the API / C ABI review, #553).

## Semantics

- **What counts:** the top index and the row and column filters of every open `SstReader` (opened lazily on first read, or pre-set by the flush or compaction that wrote the SST). Index partitions, data blocks and blob records are cached as usual and don't count.
- **Two readers of one SST** (racing openers, where the loser's reader is dropped) count twice while both live. The loser's drop ends its count.
- **Relaxed atomics** on the cache (`pinned`, an armed flag, the warning counter). The gauge is exact once open and drop calls have returned. A reading taken during them may lag.
- **The warning rule:** a rise from at or below half the capacity to above it counts one warning, then no more until the pinned bytes fall below 40% of the capacity. A disabled cache (capacity 0) counts no warnings.
- **Cost:** two atomic adds per SST reader open and drop; nothing on the read path.

## Callers

- `pigeonhole-sst`: `Reader::open` and `Drop for Reader`.
- `pigeonhole-engine`: `Engine::metrics` fills the three fields.
- `pigeonhole`: no change (`engine_metrics()` passes them through).
