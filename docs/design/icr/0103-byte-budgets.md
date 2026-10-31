# 0103: byte-budgeted reads (D308)

**Status:** Approved (number from the coordinator, 2026-10-31; owner accepted D308 at revision 2 on #1250, c6549794531, after perf287's engine review and cli692's API review). PR 1 adds the engine part; PR 2 the public API; PR 3 `Nearest::run_within`, after D296 PR 4.

## PR 1: the engine (internal crates, no promise)
- **pigeonhole-engine:**
  - `ByteBudget<I, F>` (a `Reducer`): `new(budget, size)`, where `size(cell, item)` is an item's own size, and `into_page() -> BytePage<I>`.
    - It keeps the longest in-budget prefix of the read's cells, in cell order. Each row's key counts once, on the row's first kept cell.
    - A column (every version the read selects) is admitted whole or rolled back (B1).
    - `merge` and `finish` sort and cut again.
  - `BytePage<I>` (`#[non_exhaustive]`): `items`, `bytes`, `overflow: Option<Overflow>`.
  - `Overflow`: `key` (the first cut column's first cell) and `bytes` (its size with the row key, so a page that resumes at it fits it in exactly that budget).
  - `BUDGET_ITEM_OVERHEAD` (16).
  - `Engine::reduce_into(...) -> Result<(R, ReduceStats)>`: `reduce` with the merged reducer unfinished.
  - `ReduceSpec::resume: Option<(Vec<u8>, ColumnPosition)>`: the read of that row resumes after the column, with each family's lane positioned in the row (B3). A position from another table, or whose family the read doesn't select, is `InvalidArgument`. It works for `Ranges` and `Keys` inputs.
  - `TraverseSpec::byte_budget` (0: none) and `TraverseSpec::parents_in_budget`; `TraverseStats::cut_by_budget`.
    - A node costs its key plus 16, plus 4 with parents. Seeds aren't counted.
    - The budget runs across hops in order. A cut hop keeps the in-budget prefix of its first-edge ranking, and the traversal ends with it (B4).
    - This holds for `traverse` and `traverse_async` alike.
  - The sequential reduce path pushes every group into one accumulator, so a `Break` ends the read (B2). Fanned-out reads keep D296 rev 1.2's per-group reducers.

## PR 2: the public API (`pigeonhole`, under semver)
As D308 rev 2 §4:
- **Scans and multi-gets:**
  - `Scan::read_within(bytes) -> Result<ScanPage>` and `KeysRead::read_within(bytes) -> Result<KeysPage>`;
  - each page has `rows()`, `bytes()`, `cut_by_budget()`, `next_bytes()` and `resume()`.
- **Positions:** `ScanPosition` (the last row, and its column when cut) and `KeysPosition` (the last key returned, and its column when cut; not an index, cli692's S1).
  - Both have `to_bytes`/`from_bytes` (versioned).
  - `Scan::resume_after(&ScanPosition)` and `KeysRead::resume_after(&KeysPosition)`.
- **Traversal:** `Traverse::byte_budget(bytes)`; `TraverseStats::cut_by_budget`.
- **The pinned rules:**
  - `limit` and the budget both apply, and whichever stops first wins. `resume()` is `Some` whenever the read stopped early, and `cut_by_budget()` is true only for the budget.
  - An empty cut page resumes at its own start.
  - `resume_after` outside the range clamps: a position before the range starts at the range; one past its end gives an empty page. Another table, or an unselected family, is `InvalidArgument`.
  - A vector cell's size counts what `CellRef::value()` returns.

## PR 3
`Nearest::run_within(bytes) -> NearestPage` (`neighbours()`, `bytes()`, `cut_by_budget()`, `next_bytes()`), after D296 PR 4. `run()` keeps returning `Vec<Neighbour>`.

## Why
D308 (#1250): retrieval for language models is budgeted in bytes and tokens, not rows. A read that stops at a byte budget, after ranking, returns as much of the best data as fits and reads no more than it needs.

## Callers
PR 1 adds no callers outside its tests. `pigeonhole`'s traversal builds `TraverseSpec` through `new`, so the new fields take their defaults.
