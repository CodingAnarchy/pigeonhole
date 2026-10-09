# 0013: `Cursor::skip_column`, the memtable's stale-tail index, and the resolver's switch (D194)

**Status:** Approved (coordinator, 2026-10-09), for the D194 prototype (#387). Everything is additive and off by default.

## Change

1. **`format::Cursor::skip_column(&mut self, column: &[u8]) -> Result<bool, Self::Error>`**, a provided method. Precondition: the cursor is valid and its key is in `column` (the key is `column` plus the internal-key suffix). If it returns `true`, the cursor has advanced at least one entry, past zero or more further entries of `column`, and never past an entry outside `column`: a `next` that may also pass more of the column's entries. If it returns `false`, it has not moved. The default returns `Ok(false)`.
2. **`memtable`:**
   - `Memtable::with_tail_index(self) -> Self`, called before the first insert. The memtable then keeps D194's process-local stale-tail index.
   - `MemtableReader`s from `Memtable::reader()` share the index. `MemtableReader::open` (reader processes) never has one.
   - `MemIter::skip_column` uses the index. It returns `false` when there is no index or no entry for the current node.
   - `MemIter::skips_columns() -> bool` says whether a jump is possible (an index and at least one entry), so a read of SSTs alone can leave the skip off and pay nothing.
   - The shared layout, FORMAT and `ShmLayoutVersion` are unchanged.
3. **`compaction`:**
   - `MergingCursor` and `FilteredCursor` forward `skip_column`. The merge moves only its top source, then re-sifts the way `next` does.
   - `ResolveOptions::skip_columns: bool` (default `false`): when it is set, `CellResolver` calls `skip_column` where it would step over an entry of a column it has finished (`col_skip`), and steps as today when the call returns `false`.
4. **`engine`:**
   - `Source` forwards `skip_column`; SST sources return `false`.
   - A new `Options` field, `memtable_tail_index: bool` (experimental, default `false`, `PIGEONHOLE_EXPERIMENTAL_TAIL_INDEX=1` turns it on for test and bench runs): the writer creates its memtables with the index and sets `skip_columns` on its reads.
   - No public `pigeonhole` API changes.

## Why

D194 (#387): reads of heavily overwritten rows step every superseded memtable version through the merging heap and the resolver (about 460 instructions each). The index lets the memtable source pass them in one jump. The resolver already discards every one of those entries (`col_skip`), so the jump changes how they are passed, not which entries count. Off, it costs one predictable branch where the resolver steps over a finished column.

## Callers

- `pigeonhole-compaction` (`CellResolver`, `MergingCursor`, `FilteredCursor`) and `pigeonhole-engine` (`Source`, shard memtable creation, read options): the only users.
- The other `Cursor` implementations (`BlockIter`, `SstIter`, `VecCursor`) keep the default.
- `pigeonhole-sim`, the public crate and the bench are unaffected; tests run with the environment switch on and off.
