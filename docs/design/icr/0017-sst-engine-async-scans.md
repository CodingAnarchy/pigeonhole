# 0017: async scans in `pigeonhole-sst` and `pigeonhole-engine`

**Status:** Proposed (#42, D196). Implemented in #42's PR 3; it needs approval before that PR merges.

## Change

`pigeonhole-sst`, all additive:

```rust
impl SstIter {
    /// Switches cache-only reads on or off (`ReadOptions::cache_only`).
    pub fn set_cache_only(&mut self, on: bool);
    /// Up to `max` uncached blocks the next steps past the current data block read, in
    /// order, appended to `out` (a next index partition not cached ends the walk).
    pub fn upcoming(&self, max: usize, out: &mut Vec<Fetch>);
    /// Whether the current data block is the SST's last.
    pub fn at_last_block(&self) -> bool;
}

impl SstReader {
    /// The first block a fresh cursor reads past the pinned top index, if uncached.
    pub fn first_fetch(&self, priority: Priority) -> Option<Fetch>;
}

/// Runs `f`, counting the block reads it makes from the file on this thread.
pub fn counting_file_reads<T>(f: impl FnOnce() -> T) -> (T, u64);
```

`pigeonhole-engine`, additive:

```rust
impl Engine {
    /// `scan` for an async scan.
    pub fn scan_async(&self, snapshot: &Snapshot, table: TableId, spec: ScanSpec) -> Result<ScanCursor>;
}

impl ScanCursor {
    /// The next row of an async scan (on a sync cursor: `next_row`).
    pub fn poll_next_row(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>>;
    /// Runs `f` counting its synchronous file reads into `Metrics::async_sync_reads`.
    pub fn counted<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T;
}
```

## Why

D196's third point, as the owner decided: `Scan::stream` prefetches the blocks it predicts and reads an unpredicted miss inside a step synchronously, counted (resumable steps are #398). A scan step runs inside the merging cursor and the resolver, so it cannot stop at a miss and restart as a get does. Positioning (a tablet's seeks and first cells) can be redone, though, so it runs cache-only and retries after fetching.

## Semantics

- **Positioning.** A tablet's cursors open their SSTs and seek cache-only (`scan_sources_into::<true>`, `open_cache_only`). A miss undoes the tablet (its lanes are recycled, `next_tablet` restored), fetches, and positions again. Once positioned, the cursors read normally (`set_cache_only(false)`, which also covers a level cursor's later SSTs).
- **One prediction for every scan (coordinator decision).** `SstIter::upcoming(max, out)` is the only predictor of what a scan reads next. The async stream passes `max = 1`. ctr274's sync scan readahead hint (#402 PR 5) builds on it: from a sync scan's miss path it takes the next N blocks and submits their `Fetch`es, admitting each in the completion's continuation. The existing sync `ReadOptions::readahead_blocks` path should move onto it there, so there are not two predictors.
- **Prefetch.** Before each step the stream asks each SST source for `upcoming(1, …)`: the open SST's next data block, or the next index partition's first block. A level cursor on its SST's last block asks for what opening its next SST and reading that SST's first block need (`reader_cache_only`, then `first_fetch`). The stream fetches it, keeps it pinned until the step has run, and asks again. That is one block ahead per cursor, and only as the stream is polled.
- **The step** runs synchronously inside `counting_file_reads` (block reads) and the engine's async-read scope (blob reads). What it reads from the file is counted in `Metrics::async_sync_reads`. Reading the row's cells after `poll_next_row` runs inside `counted` the same way.
- **Fallbacks.** If a fetched block is not kept (a cache of size 0), or a tablet needs more than 64 fetches to position, the scan reads synchronously from then on (counted).
- **Sync scans are unchanged.** `scan_sources_into` and `open_next_tablet` take the tier as a const generic (`::<false>` for sync callers). `advance_row` is `advance_row_impl::<true>`. `note_file_read` is a cold thread-local check on the file-read paths only (`read_block`'s miss, `read_uncached`, `read_run`).

## Callers

- `pigeonhole-engine`: `source.rs` (`sst_sources_range` and `scan_sources_into` generic over the tier; `Level::open` opens cache-only when its options say so; `SstSource` and `Source` gain `set_cache_only` and `upcoming`), `read.rs` (`ScanCursor` gains the async state, `poll_next_row` and `counted`; `open_next_tablet` and `advance_row_impl` are generic), `engine.rs` (`scan_async`).
- `pigeonhole`: `Scan::stream` and `nonblocking::RowStream` (behind `async`, which now depends on `futures-core`).
