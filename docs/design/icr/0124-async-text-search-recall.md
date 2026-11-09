# 0124: async text search and memory recall (#1560)

**Status:** Approved (number from the coordinator, 2026-11-05). The design note is on #1560 (issuecomment-6669876325), agreed by perf287 (text hot path, T1–T3) and ai1249 (memory and bindings, §10).

## The change
- **pigeonhole-text** (semver, additive, the `async` feature):
  - `Search::run_async(self) -> Result<Vec<Hit>>`;
  - `Search::run_with_stats_async(self) -> Result<(Vec<Hit>, SearchStats)>`.
  
  Both are `pub async fn`, with `Send` futures, and have the same refusals, hits and stats as `run`.
- **pigeonhole-memory** (semver, additive): `Recall::run_async` and `Recall::run_with_stats_async`, with the same contract as `run`.
- **pigeonhole** (semver, additive, the `async` feature):
  - `RowCells::next_async(&mut self) -> Result<Option<CellEntry<'_>>>`, a lending `pub async fn`;
  - `RowCells::poll_next(&mut self, cx)`, which it wraps;
  - `RowCells::poll_seek(&mut self, cx, family: &str, qualifier: &[u8])`: the first cell at or after a column, never moving back.
- **pigeonhole-engine** (internal 2.x API, additive):
  - `ScanCursor::poll_next_cell(&mut self, cx) -> Poll<Result<bool>>`;
  - `ScanCursor::poll_seek_cell(&mut self, cx, family, qualifier) -> Poll<Result<bool>>` (D196 within a row).
  
  #1625 also adds engine calls so a walk built sync can go on async: `Engine::async_scan_from`, `ScanCursor::families`, `ScanCursor::is_async`, `ScanCursor::cell`, and `ScanCursor::seek_cell` (the sync seek that finishes a pending one, ai1249's R1).

## Semantics
- **A walk within a row fetches before it steps.**
  - The stepping lane's next block is fetched one ahead if uncached, probed once per block it stands in.
  - A seek moves its family's cursors cache-only, fetching the block it lands in on a miss.
  - A `Pending` changes nothing the walk shows, and a seek left `Pending` is finished by the next step, async or sync.
  - A block still missed is read synchronously and counted in `Metrics::async_sync_reads`.
- **Text and memory run the same cores as their sync forms,** generic over their reads, so the results are identical. The sync forms keep their codegen: a const-generic yield budget compiles out of them.

## Why
#1479's tier 2: an async server or agent loop must not block an executor thread on a text search's or a recall's reads.

## Callers changed
None: additive. The sync `run`, `RowCells::next` and `ScanCursor::next_cell` are unchanged.
