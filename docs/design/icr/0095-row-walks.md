# 0095: walking a wide row cell by cell, in pages, and resuming by position

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D297 at revision 2 on #489). PR 1 (#1210) adds the sync calls; PR 2 adds the async ones below.

## PR 1: the sync walks
- **pigeonhole:**
  - `RowRead::cells(self) -> Result<RowCells<'t>>`, `RowRead::pages(self, n: usize) -> Result<RowPages<'t>>` and `RowRead::resume_after(self, &RowPosition) -> Self` (which `read()` honours too).
  - `RowCells::next(&mut self) -> Result<Option<CellEntry<'_>>>` and `RowCells::position(&self) -> Option<RowPosition>`; `RowPages::next(&mut self) -> Result<Option<RowRef<'_>>>` and `RowPages::position`. Both lend until the next call.
  - `Scan::cells(self) -> Result<ScanCells<'t>>`, with `ScanCells::next(&mut self) -> Result<Option<(&[u8], CellEntry<'_>)>>`.
  - `RowPosition` with `to_bytes(&self) -> Vec<u8>` and `from_bytes(&[u8]) -> Result<Self>`: byte 0 the encoding version (1), the table and family ids (`u32` LE), the qualifier length-prefixed. A position from another table, or in a family the read does not select, is `InvalidArgument`; a qualifier over 64 KiB is refused at decode.
- **Internal crates** (no promise): **pigeonhole-engine** `ColumnPosition` (`new`, `table`, `family`, `qualifier`, `to_bytes`, `from_bytes`).

## PR 2 (planned): async
`poll_next` and `next_async` on `RowCells`, `RowPages` and `ScanCells` (lending), and `into_owned_stream()` behind the `async` feature (`CellStream`, `PageStream`); `into_owned_iter()` on the three types.

## Why
D297 (#489 item 1): a graph hub's row read allocated in proportion to its edges (824 MB for 2M edges); the walks read it in O(blocks) memory.

## Callers changed
None: additive. The pigeonhole model target gains walk modes (cells, pages, resumed pages).
