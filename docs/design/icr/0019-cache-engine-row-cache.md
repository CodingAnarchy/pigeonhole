# 0019: the row cache in `pigeonhole-cache` and `pigeonhole-engine`

**Status:** Approved (coordinator, 2026-10-10; #404, D201).

## Change

`pigeonhole-cache`, additive:

```rust
/// Write watermarks for the row cache's epochs, indexed by a caller-computed row hash.
pub struct RowEpochs;

impl RowEpochs {
    pub fn new(slots: usize) -> Self;
    /// Raise the row's watermark to `seqno` (Release), before the write can be visible.
    pub fn note_write(&self, hash: u64, seqno: u64);
    /// The row's epoch for a read at `seqno` (Acquire): `None` if a write to it is applied
    /// but not visible at `seqno`.
    pub fn epoch(&self, hash: u64, seqno: u64) -> Option<u64>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

`pigeonhole-engine`, additive:

```rust
pub struct EngineOptions {
    // ...
    /// Largest encoded family row the row cache stores (default 4 KiB).
    pub row_cache_max_row: usize,
    /// `(table, family)` names the row cache serves; empty means every family.
    pub row_cache_families: Vec<(String, String)>,
}

pub trait RowSink {
    // ... (existing methods)
    /// How many cells the sink holds (`None` by default: the sink cannot report them, and
    /// its reads are not stored in the row cache).
    fn cell_count(&self) -> Option<usize> { None }
    /// Cell `i`: its qualifier and data.
    fn cell(&self, i: usize) -> Option<(&[u8], &CellData)> { None }
}

impl Engine {
    /// Hits, misses and fills since open; zero when the cache is off.
    pub fn row_cache_stats(&self) -> RowCacheStats;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowCacheStats { pub hits: u64, pub misses: u64, pub fills: u64 }
```

`RowData` implements the two new `RowSink` methods, and so does `pigeonhole`'s `RowBuf`.

## Why

D201. The engine supplies `RowCache`'s epoch from a per-row write watermark, which needs a shared table written by shards and read by readers. It lives in the cache crate next to `RowCache`, so its loom model runs with the cache crate's existing loom setup. A miss reads the family row through the unchanged `read_row_into` (so its codegen with the cache off doesn't move, #430), then reads back what it pushed into the caller's sink to store it. That needs the sink to report its cells, hence the two provided methods. Defaults keep every other implementor working, and a sink that doesn't report just doesn't fill.

## Semantics

See D201: the watermark protocol, what's served (latest reads, newest version, no time range; projections applied to cached cells), what's stored (unprojected reads under `row_cache_max_row`), gets consulting without filling, TTL deadlines, and the reader-process exclusion.

## Callers

- `pigeonhole-engine`:
  - `row_cache.rs` (new: `RowCaches`, the encoding, `read_row_cached`, `get_cached`);
  - `shard.rs` (`Shared::row_cache`; `apply` calls `note_write` once per row);
  - `engine.rs` (`read_row_latest_into` and `get_latest` consult the cache; `row_cache_stats`);
  - `nonblocking.rs` (latest `RowFuture` and `GetFuture` do the same);
  - `read.rs` (`RowSink` read-back; `CellData::copied`).
- `pigeonhole`: `Options::row_cache_max_row` and `row_cache_family`, `Pigeonhole::row_cache_stats`, `RowCacheStats`, and `RowBuf`'s read-back.
