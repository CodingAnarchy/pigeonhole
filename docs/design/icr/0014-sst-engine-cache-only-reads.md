# 0014: cache-only reads in `pigeonhole-sst` and `pigeonhole-engine`, for async reads

**Status:** Approved (coordinator, 2026-10-09; #42, D196). Implemented in #42's PR 2a (#411).

## Change

`pigeonhole-sst`, all additive except the new `ReadOptions` field:

```rust
pub struct ReadOptions {
    // ...existing fields...
    /// Read only what is cached: a block that is not fails the read with
    /// `Error::WouldBlock` instead of being read from the file.
    pub cache_only: bool, // default false
}

pub enum Error {
    // ...existing variants...
    /// A cache-only read missed: the block it needs.
    WouldBlock(Box<Fetch>),
}

/// One read of the file a cache-only read missed, and how its bytes enter the cache.
pub struct Fetch { /* private */ }
impl Fetch {
    pub fn submit(&self) -> pigeonhole_io::Completion;              // the VFS's async read(s)
    pub fn admit(&self, buf: IoBuf) -> Result<Option<BlockHandle>>; // verify, keep, pin
    pub fn is_kept(&self) -> bool;                                  // false if the cache keeps nothing
}

impl SstReader {
    /// As `open`, from the block cache only (footer, top index, filters, properties).
    pub fn open_cache_only(file, meta, cache, priority) -> Result<Self>;
}

impl BlobReader {
    /// The value if its record is cached; never reads the file.
    pub fn cached(&self, ptr: &BlobPointer) -> Option<Cell>;
    /// The cached value; or `WouldBlock` with the fetch it needs (an extent header not
    /// verified yet, then the record); or `None` for a record too large to cache.
    pub fn read_cache_only(self: &Arc<Self>, ptr: &BlobPointer) -> Result<Option<Cell>>;
}
```

`pigeonhole-engine`:

```rust
pub enum Error { /* ... */ #[doc(hidden)] WouldBlock(Box<pigeonhole_sst::Fetch>) } // internal

impl Engine {
    pub fn get_latest_async(&self, table, family, row, qualifier) -> GetFuture;
    pub fn get_async(&self, snapshot: &Snapshot, table, family, row, qualifier) -> GetFuture;
    pub fn read_row_latest_async<S: RowSink + Clone + Unpin>(&self, table, row, families, spec, sink: S) -> RowFuture<S>;
    pub fn read_row_async<S: RowSink + Clone + Unpin>(&self, snapshot, table, row, families, spec, sink: S) -> RowFuture<S>;
}
pub struct GetFuture;    // Future<Output = Result<Option<CellData>>>
pub struct RowFuture<S>; // Future<Output = Result<Option<S>>>

pub struct Metrics { /* ... */ pub async_sync_reads: u64 } // non_exhaustive already
```

## Why

D196: async reads must not block their executor thread on a block the cache does not hold, and must not use `spawn_blocking`. Every read below the engine went through `read_at`. A read that can stop at a miss and say what it needs is the smallest change that lets a future fetch the block through the VFS's existing asynchronous read (`submit_read` → `Completion`), admit it, and read again. RocksDB's `kBlockCacheTier` read tier is the same idea.

## Semantics

- **Sync callers are unchanged.** They read with `cache_only: false`. The tier is checked only after a cache miss, so a hit runs the same code. A sync open reads its footer directly, as before, and the cache-only open is a separate entry point.
- **A cache-only open caches its footer** (key: the SST's cache file at the footer's offset), so a retry finds it. It fills the cache with the properties block (a sync open does not), for the same reason.
- **Readahead never runs in cache-only mode:** a miss on the first block of a run is a `WouldBlock` for that block.
- **The futures** take their read point on the first poll: the latest view as `get_latest` reads it (D188, #315), or the snapshot given; a reader process takes a snapshot, with the expired-snapshot retry. They keep fetched blocks pinned until they resolve.
  - They fall back to one synchronous attempt, counted in `Metrics::async_sync_reads`, when a fetched block is not cached afterwards (capacity 0, or a block larger than a cache shard), or after 64 fetches.
  - A separated value is fetched like a block (#42's PR 2b): each blob extent header it touches, verified once, then the record, whose pieces (a record can span extents) are read and joined into one completion. One above the blob cache limit is read synchronously and counted (D196 option (a), #398).
- **A row read restarts from an empty copy of its sink** (the futures require `S: Clone`). An earlier draft added a required `RowSink::clear`, which `cargo semver-checks` rejected against 0.2.0 as a breaking change, so `RowSink` is unchanged.

## Amendment in #42's PR 2b (approved, coordinator, 2026-10-09)

The approved surface changed in three ways, all inside `pigeonhole-sst` and its one caller (the engine's async futures):
- `Fetch::admit` returns `Option<BlockHandle>`: a fetched blob extent header is kept as a verified flag, not as a block.
- `Fetch::is_cached` is renamed `is_kept`.
- `BlobReader::read_cache_only` is new.

A `Fetch` may now carry several pieces (a blob record spanning extents). `submit` still returns one completion.

## Callers

- `pigeonhole-engine`: `source.rs` (`read_options`, `sst_sources_point`, `sst_sources_row`, `View::point_sources`, `View::row_sources_into` take `cache_only`), `snapshot.rs` (`OpenSst::reader_tiered`, `SstSet::read_pointer` counts synchronous blob reads inside async reads), `read.rs` (`get_in`, `get_with`, `read_row_into`, `read_row_with` take `cache_only`; sync callers pass `false`), `shard.rs` (its own point read passes `false`), the new `nonblocking.rs`.
- `pigeonhole`: `Table`/`ReadTable::get_async` and `get_at_async`, `RowRead::read_async`, `Pigeonhole`/`PigeonholeReader::async_sync_reads` (behind `async`).
- No other crate constructs `ReadOptions` with a struct literal (the engine uses `Default` and sets fields).
