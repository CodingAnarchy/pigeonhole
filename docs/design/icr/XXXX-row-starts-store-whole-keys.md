# XXXX: row starts store their whole key (#759)

**Status:** Proposed (blob33, 2026-10-28); number pending from the coordinator. Not approved.

## Change

A writer-side choice within format 4, plus a reader fast path. Nothing is added, removed or renamed in any API.

- **Writer (`pigeonhole-format`, `BlockBuilder::add`):** a data-block entry that starts a row stores `shared = 0`, its key whole, as restart entries do. Before, it stored the prefix it shared with the previous key (`shared <= ` the previous row prefix's length). Restart points and both tables are unchanged.
- **Reader (`BlockIter::seek`):** when the target is a whole row prefix (`row_prefix_len(target) == target.len()`) and the block has row starts, it binary-searches the row-start table on the full keys there and positions on the answer: the first row start at or after the target's row, or entry 0 when the block begins inside a row at or after it. If it meets a row start with `shared != 0` (a block written before), it falls back to the restart search, with the cursor untouched. Other targets, and index blocks, seek as before.
- **FORMAT.md §4.2:** `shared` is defined as at most the common prefix (every reader already decodes it that way), and records that row starts store 0 from this change.

## Compatibility

No format version or feature bit. Every reader since format 1 takes `shared` as written: a smaller `shared` is a valid encoding of the same key. So 1.x builds before this change read the new blocks, and this build reads the old ones (the frozen `data_block.bin` golden stays and is seek-checked; the new encoding is the frozen sibling `data_block_row_starts.bin`).

## Why

#759 (row-read p99 vs SQLite). A cached row read spends about half its CPU in `BlockIter::seek`, per source: a binary search over restarts (every 16 entries), then a walk over up to 15 entries that rebuilds each key (`KeyBuf::extend_from_slice`) and compares it. With whole keys at row starts, the search ends on the row's first entry, with no walk and no key rebuild. Cost: the bytes a row start used to share, 0.6–0.8% of a YCSB-shaped data block before compression (10 × 100 B and 1 × 1000 B rows, 4 and 16 KiB blocks).

## Callers

- **`pigeonhole-format`:** `block.rs` (`BlockBuilder::add`, `BlockIter::seek`, `seek_row_start`); tests `tests/block.rs` (`row_seek_matches_model_across_blocks` against a model and against old-style blocks, `row_seek_in_a_block_that_continues_a_row`), `tests/golden.rs` (`blocks`), `tests/format_literals.rs` (`logical_data_block`).
- Every block reader goes through `BlockIter::seek`; no other crate changes.
