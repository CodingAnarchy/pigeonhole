# 0004: `format::block` — O(1) `Block::new`, `Block::validate`, `BlockIter::reset`, inline key buffer

**Status:** Approved (coordinator, 2026-10-06). Implemented in PR #30. (The review named this "ICR 0003"; that number was already taken by the sim model request, so it is 0004.)

## Change

1. `Block::new` checks only what is O(1): the table counts fit the block and the first restart is at offset 0. Restart and row-start offsets are checked when a cursor uses them (an out-of-range offset is `Corrupt`), so a cursor never panics on bad bytes.
2. New `Block::validate(&self) -> Result<()>`: the old up-front check that both tables are strictly ascending and point inside the entries. Used where a whole block should be vetted: the SST reader validates the top index once at open, and the fuzz/arbitrary-input harness calls it.
3. New `BlockIter::reset(&mut self, bytes: B) -> Result<()>`: moves the cursor onto another logical block (unpositioned) with the O(1) checks, keeping its key buffer. On error the cursor holds `bytes` as an empty block and is invalid.
4. `BlockIter` rebuilds non-restart keys in a buffer that is inline for keys up to 128 bytes (heap beyond), so iteration allocates nothing for typical keys even with a fresh cursor per lookup.
5. `skip_row` checks that the row start it follows lies after the current entry (defensive; the binary search already guarantees it).
6. Additive: `BlockIter: Clone` (for `B: Clone`) and `BlockIter::block()`.

## Why

`pigeonhole-sst` needs an allocation-free hot point lookup and O(1) block opens. It had a private copy of the decoder to get them. The review asked for one decoder, so `format::block` gained these properties and the copy was deleted.

## Callers

- `pigeonhole-sst` (`SstIter`, `SstReader::open`): the only production user.
- `pigeonhole-format`'s tests (`tests/block.rs`, `golden.rs`, `format_literals.rs`), the fuzz harness (`tests/common/harness.rs`, now also calling `validate`) and the `format` bench. None relied on `Block::new` rejecting unsorted tables.
