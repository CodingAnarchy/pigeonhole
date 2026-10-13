# 0041: SST sources pass a finished column's entries with `Cursor::skip_column` (amends ICR 0013)

**Status:** Approved (coordinator, 2026-10-13; #387). Amends [ICR 0013](0013-stale-tail-skip-column.md), whose `BlockIter`, `SstIter` and engine SST sources kept the default `skip_column` (`false`).

## Change

The `Cursor::skip_column` contract (ICR 0013) is unchanged. More cursors implement it:

1. **`format`:** `BlockIter::skip_column` passes the column's later entries in its block by reading their headers only. An entry is in the column when it shares at least the column's length with the key before it and has a column key's length. A restart entry (shared prefix 0) is compared with the column directly. Only the entry it stops on is decoded. It always moves at least one entry and returns `true`. At the block's end it becomes invalid, as `next` does.
2. **`sst`:** `SstIter::skip_column` forwards to its data block. At the block's end it moves to the next block, where the column may continue, and settles its filter as `next` does.
3. **`engine`:**
   - `SstSource` (an SST, or a level's SSTs) forwards it and moves to the level's next SST at an SST's end.
   - `Source::Sst` forwards it instead of returning `false`.
   - `skips_columns` sets `ResolveOptions::skip_columns` whenever a read has an SST source, not only an indexed memtable.
   - The test hook `Engine::sst_skips()` counts SST skips.

No FORMAT, on-disk or public API change.

## Why

#387: a finished column's superseded versions in SSTs are stepped one entry at a time, each through the merging heap and the resolver. On the hot row as written, 0.64 `skip_column` calls per cell landed on an SST and returned `false`. Flushed, with no memtable, `skip_columns` was off and 0.87 stale SST entries per cell were stepped. D194's correctness argument (point 2) holds for any source: the skip runs only once the resolver has finished the column (`col_skip`), where every remaining entry of the column, from every source, is discarded anyway.

## Callers

- `pigeonhole-compaction`'s `CellResolver` (the only caller of `skip_column`), through `MergingCursor` and `FilteredCursor`, which forward it already.
- Compaction, flush and backup cursors don't set `skip_columns` and are unaffected.
- `VecCursor` and the memtable keep their implementations.
