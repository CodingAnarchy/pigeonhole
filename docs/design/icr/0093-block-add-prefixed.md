# 0093: `BlockBuilder::add_prefixed`, a data-block entry with the caller's common prefix and row length

**Status:** Approved (number from the coordinator, 2026-10-29; the perf-nit audit's SST writer finding, design by simp724, format half by blob33). Additive API on `pigeonhole-format`, an internal crate under D282. Nothing is removed or changed, and the file format is untouched: the bytes written are the ones `add` writes.

## Change
- **pigeonhole-format:** `BlockBuilder::add_prefixed(&mut self, key: &[u8], value: &[u8], lcp: usize, row_len: usize) -> Result<()>`, for data-block builders only.
  - **Caller's contract:** `lcp` is the common prefix length of `key` with the previous key added (including the last key of the previous block, across `reset`), and `row_len` is `key`'s row prefix length. The caller has already checked that `key` sorts after the previous key.
  - **What the builder skips:** its own `common_prefix_len`, the order check, `row_prefix_len` on a new row, and its `last_key` copy.
  - **What it still does:** the same restarts, `shared` bytes and row starts, deriving "same row" from its own `last_row_len` as `add` does, so row starts stay right across `reset`. It refuses an index builder, and keeps the `finished` and block-size checks.
  - **`last_key()`:** stale on a builder fed by `add_prefixed`; it panics in debug builds if called on one. A builder is fed through one of the two methods, never both. The SST writer calls `last_key()` only on its index builder, which keeps using `add`.
- **The ledger entry said "debug_assert against its own computation".** That was dropped by agreement with simp724: it would need `last_key` maintained in debug builds, which allocates on paths the alloc-budget tests count (the test profile has debug assertions). A property test stands in for it (see below).

## Why
`SstWriter::add` already computes the common prefix with the previous key (for its order check and row tracking) and the row prefix length. `BlockBuilder::add` computed both again for the same previous key and kept a second copy of it: about 30–60 instructions per cell in every flush and compaction (perf-nit audit, 2026-10-29).

## Callers changed
- **This change:** none. `add` stays for index blocks and every other caller.
- **simp724's follow-up:** `SstWriter`'s data builder moves to `add_prefixed`, passing its `lcp` and the escaped row plus terminator (`row_len + 2`). It keeps its order check and its `last_key`.

## Tests
`crates/format`: a proptest feeds the same sorted keys (rows of several cells, rows split across blocks, long shared prefixes) through `add` and through `add_prefixed`, with the caller's `lcp` and `row_len`, across resets and restart intervals. It asserts byte-identical `finish()` output for every block.
