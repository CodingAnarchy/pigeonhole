# 0080: `SstIter<const LAZY: bool>` and `SstReader::iter_lazy` (D288 PR 2)

**Status:** Approved (coordinator, 2026-10-27; D288, reviewed by perf287 on #711, comment 6433582625). Additive: a defaulted const parameter on `SstIter` and a new `SstReader` method. Every existing use of `SstIter` names the eager cursor, unchanged.

## Change

**`pigeonhole-sst`:**

- `pub struct SstIter<const LAZY: bool = false>`. `SstIter` (the default) is the eager cursor, as before. The lazy paths are behind the const parameter, so the eager cursor's code is unchanged.
- `SstReader::iter_lazy(self: &Arc<Self>, filter: ScanFilter, options: ReadOptions) -> SstIter<true>`: a cursor for a source under a merge (`pigeonhole-compaction`'s `MergingCursor`, D288).
  - **With a filter to apply,** a move lands without filtering past its landing entry. That entry is checked once and the verdict kept, as `FilteredCursor::lazy` does.
  - **It stops before loading a data block,** parked on a lower bound of what follows:
    - the separator of the block it leaves (the writer keeps `last <= sep < next`);
    - or the target of a seek hint or row skip past its block.
  - **It implements `Cursor::settled`, `settle_until` and `may_park`.** `settle_until(bound)` loads a parked block only once `bound` reaches its lower bound.
  - **Under `ScanFilter::all`** it is the eager cursor.

## Why

D288 (#711): each filtered source of a scan settled on its own to its next admitted entry, walking (and, for an SST, reading) every block past the merge's position when it had nothing admissible near it. PR 1 made memtable sources lazy. An SST source needs the parked state inside the cursor that loads its blocks: a wrapper above `SstIter` could only park on an entry the cursor had already loaded.

- **Measured:** #711's shape over SSTs (`compaction/tests/lazy_sst.rs`) reads 6 blocks lazily against 643 eagerly.

## Callers

- **`pigeonhole-engine`:** `source.rs`.
  - `sst_source` builds a range scan's and a row read's SST sources lazy when the filter has anything to reject. These are `Source::LazySst(SstSource<true>)`, beside `Source::Sst`, which point gets and unfiltered reads keep.
  - A lazy level opens its SSTs through `OpenIter`, and parks before its next SST on that SST's smallest key.
- **Tests:** `compaction/tests/lazy_sst.rs`; `engine/src/source.rs`'s level test.
- **`cargo semver-checks`:** a defaulted const parameter and an added method (minor).
