# 0026: `BufPool`, recycled `IoBuf`s

**Status:** Approved (coordinator, 2026-10-10; #372). The coordinator approved the plan with one change: the pool return rides inside an existing `BlockData` variant (`Io(IoBuf)`), so `pigeonhole-cache` gains nothing. A new `BlockData` variant would break semver on the published 0.2.

## Change

`pigeonhole-io`, additive:

```rust
/// A bounded pool of heap `IoBuf`s: a buffer taken from it goes back when dropped
/// (unless the pool is full), so a reader that decodes block after block reuses a few
/// allocations instead of allocating and zero-filling one per block.
pub struct BufPool { /* private */ }

impl BufPool {
    /// A pool keeping at most `max` buffers.
    pub fn new(max: usize) -> Arc<BufPool>;

    /// A buffer of `len` bytes: a recycled one truncated or extended to `len` (only the
    /// extension is zero-filled), or a new zeroed one when the pool has none. Its bytes are
    /// initialized (zeros or whatever a previous user wrote), for the caller to overwrite.
    pub fn take(self: &Arc<Self>, len: usize) -> IoBuf;
}
```

`IoBuf` gains a private field (an optional `Arc<BufPool>`): one word, so an `IoBuf` grows from 40 to 48 bytes. Its `Drop` hands a pooled buffer back by swapping itself for an empty buffer and pushing the original (with the handle cleared, so the pool holds no `Arc` of itself) into the pool. That is safe code over the existing allocation logic: no new `unsafe`. `IoBuf::detached` keeps a pooled buffer as is (it is heap memory; it still goes back when dropped).

## Why

#372, part of #320: compaction spends about 3.9% of its instructions zero-filling a new decompression buffer per uncached block (`sst::reader::decode_slice`'s `vec![0; n]`), which is freed right after the block is passed. The safe LZ4 decoder (`lz4_flex` with `safe-decode`, the workspace's choice) needs an initialized output, so the fill can only be avoided by reusing a buffer that is already initialized. The buffer comes back wherever its last handle drops: on the compaction thread, on an I/O completion thread after readahead, or later when a `Cell` held past its block drops. So it covers the single-block, readahead and prefetch paths without passing buffers back through `BlockIter`.

## Semantics

- **Bounded.** A buffer dropped into a full pool is freed as before. Buffers with a kept prefix (`IoBuf::keep`) are freed, not pooled.
- **No stale bytes reach a reader.** `take` hands out initialized memory. The sst decoder writes the whole buffer: `format::compress::decompress` fails a block unless the codec wrote exactly `out.len()` bytes.
- **Registered slots are unchanged** (`SlotPool`, #402). A buffer is either a slot or heap, and only heap buffers are pooled.

## Callers

- `pigeonhole-sst`: the uncached read paths decompress into a buffer from a per-reader pool, created on the reader's first read that does not fill the cache (an empty pool: nothing is allocated until a block is decoded) and bounded at 6 buffers (compaction's readahead of 4, the block its cursor is on, and the one being decoded). Cached blocks keep their exact-size heap buffers (the cache charges capacity, and a page-rounded `IoBuf` would cost cache space). A merged readahead run's per-block pieces, which it overwrites whole, come from the same pool for such reads.
- No other caller changes.
