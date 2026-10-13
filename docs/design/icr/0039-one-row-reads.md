# 0039: `ReadOptions::one_row` and row-end index separators: a point or single-row read whose row ends a data block no longer reads the next block

**Status:** Approved (coordinator, 2026-10-12; the cold-get breakdown, #406). Revised after CI callgrind: the first version passed the read's upper bound down the cursor stack (`Cursor::set_upper_bound`), which cost 20–70 instructions per read and per scan row. This version costs one option per source.

## Change

`pigeonhole-sst`. Both changes are additive: `ReadOptions` is `#[non_exhaustive]`, and SST bytes stay within FORMAT §4.2.

```rust
pub struct ReadOptions {
    // ...
    /// The read stays within one row: its caller stops at the first key past the row of the
    /// keys it reads (a point get or a single-row read).
    pub one_row: bool, // default false
}
```

- **The writer:** between data blocks whose last and first keys are in different rows, the index separator is now at least the row-end bound of the first block's row: its row prefix with the terminator's last byte bumped (`escaped row ++ 00 02`). It is kept as is when the shortest separator already reaches past the row.
  - It still lies in `[last, next)`, so FORMAT §4.2 is unchanged.
  - Neighbouring rows that differ by one in a byte (`…1234`, `…1235`: dense numeric keys) used to get `last` itself, which says nothing about where the row ends.
- **The SST cursor, with `one_row`:** stepping off a data block whose separator is a row-end separator, it ends instead of loading the next block. Every key there sorts after the separator, so after the row of the block's last key. On the paths that step (next, the filter's skips, forward seeks), the cursor has already reached the read's row, so that row is at or after the read's.
  - Not after a fresh seek, where the block's keys may all precede the read's row.
- **`pigeonhole-engine` (crate-private):** point and row reads (`sst_sources_point`, `sst_sources_row`) set `one_row`. Scans don't.

## Why

The 2026-10-12 gate measured 1.106 device reads per cold get against "one I/O" (#406). Counting block reads by kind (a 4 GiB table built the gate's way, cache 1/64, direct I/O):
- the row's data block: 1.000;
- the next data block of the same SST: ~0.04;
- filter false positives: ~0.01;
- index partitions: ~0.

A row read learned that its row had ended only from the next key. When the row's cell ended a block, that meant reading the next block.

## Effect

`coldget` on that table, base → this change (each build writes its own SSTs), 200K gets, direct I/O:

| warm-up | present gets: reads per get | absent gets |
|---|--:|--:|
| 100K | 1.059 → 0.992 | 0.011 → 0.011 |
| 1M | 1.053 → 0.985 | 0.011 → 0.011 |

Below 1.0 because some rows' blocks are already cached from the warm-up.

**Compatibility:** SSTs written before this keep their separators, which are never row-end ones, so their reads are unchanged until compaction rewrites them.

## Checked by

- `sst` `a_one_row_read_never_reads_the_block_after_its_row`: 800 sequential rows in 256-byte blocks. Every one-row read makes exactly its seek's reads and returns the same entries; without the option some rows step over.
- `sst` writer `separators_between_rows_reach_past_the_first_row`: a property over internal keys with escapes and shared prefixes.
- Mutation checks: each test fails with its half of the change removed.
