# Compaction questions (Phase 2)

## Proposed decision: the tiered picker's runs, triggers and output levels (#31)
The spec says only "tiered/universal for write-heavy families". `Levels` keeps L0 overlapping and newest first and every deeper level sorted and disjoint, so a merged run must land in a whole level.

**Interim behavior:**
- The sorted runs are each L0 file and each non-empty deeper level, newest first.
- Once L0 holds `l0_trigger` files, all of them merge, taking in the following level runs while each is at most `PickerOptions::tiered_size_ratio_percent` (default 1) larger than what was taken so far. Level 1 is always taken when it is not empty, since the output must go above the first run left out.
- Once the runs above the oldest one hold more than `PickerOptions::tiered_max_space_amp_percent` (default 200) of its bytes, every run merges into the last level.
- The output goes just above the first run not taken (the deepest free level), or to the last level when every run was taken. Runs therefore stay ordered newest first down the levels, so GC's `bottommost` and `min_ts_above` mean what they do for leveled. Whole runs move, so no row is ever split (D78 holds trivially).
- A lone L0 file over levels it does not need to merge with is a `TrivialMove`.
- `score`, which drives picking, is the larger of L0 depth over `l0_trigger` and space amplification over its cap.
- The two `PickerOptions` fields are additive (`PickerOptions` is `#[non_exhaustive]`), engine-wide like the leveled knobs; the public crate exposes none of them.

## Q: tiered write amplification once L1 holds a run (#31 review)
Because L1 is always taken when it holds a run, every L0 merge after that rewrites all of L1. L1 grows by one L0 batch per merge until the size ratio takes in L2 or space amplification fires, and it can reach about 2× the last level first. Over k merges that rewrites up to k batches each, so write amplification grows roughly quadratically in k, where universal compaction's is logarithmic.

**Proposal:** accept this for now. Tiered is still opt-in, and the public crate refuses it until #44. Bound it in [#228](https://github.com/CodingAnarchy/pigeonhole/issues/228) (Phase 2). The preferred fix there is sorted runs in L0, as RocksDB universal does, which is a manifest/format change. The format-free alternative is to push L1..Lk down into a free level before L1 would be forced.

**Interim behavior:** as described; the picker proptest bounds run count and space amplification but not write amplification.

## Proposed decision: the write stall follows L0 depth only (#31 review; D119)
The engine set the stall score to the highest picking score among the shard's slots. That already let a leveled deeper level over its target pace writers. With tiered's space amplification included, a fresh tree with three equal L0 files (200% amplification, the default cap) would have stalled writers below `l0_trigger`, for the length of a full-tree merge.

**Interim behavior:** the added `CompactionPicker::stall_score` returns L0 depth over `l0_trigger` for every style, and the stall uses only that. `score` (L0, deeper levels, space amplification) only decides which slot compacts first. For leveled families this narrows the stall to L0, as D119 describes it.

## Proposed decision: the engine picks per family (#31)
The engine built one leveled `CompactionPicker` per shard and used it for every family.

**Interim behavior:** each shard keeps a leveled and a tiered picker and scores and picks each `(tablet, family)` slot with its family's style (a `match`, so a new style fails to compile). `FifoByTime` families keep compacting leveled until #32's picker lands (engine-level only; the public crate refuses the style, D95). Full compactions (`Engine::compact`) and #95 cleanups still merge everything into the last level whatever the style. The engine model-check harness gives family `g` the tiered style, so every suite and seed sweep runs both pickers against the oracle; runs that turn background compaction off for deterministic purges also set `tiered_max_space_amp_percent = u32::MAX`.

## Proposed decision: the FIFO-by-time picker (#32)
The spec says "whole SSTs drop when their newest timestamp expires, with no rewrite". Issue #32 also asks for a size-based fallback and for a decision on tombstones in dropped SSTs.

**Interim behavior:**
- **Expiry.** `pick(.., now, ttl_micros)` returns one `TaskKind::Drop` of every SST, at any level, whose newest timestamp has expired (`ts_range.1 + ttl <= now`, the model's TTL rule), unless it is busy. A drop has no job and no I/O. Reads at every snapshot are unchanged from `now` on: every entry in the SST has expired, and so has everything one of its tombstones hides, since a delete at `T` hides only timestamps `<= T` (D74). A dropped tombstone therefore never uncovers live data. No extra rule is needed, and the drop is not a purge, so no `CompactionRecord` is kept.
- **Time-aware score.** The added `CompactionPicker::score_at(levels, now, ttl_micros)` scores at least 1.0 while an expired SST exists. `score(levels)` is `score_at(levels, 0, 0)`. The engine calls `score_at` with the shard clock and the family's TTL. Expiry is noticed only when the slot's maintenance runs (after a manifest commit), not by a timer: an idle family keeps its expired SSTs, invisible to reads, until its next flush or compaction.
- **Size cap.** The added `PickerOptions::fifo_max_bytes` (default 0: none, engine-wide). Past it, the SSTs with the oldest newest timestamps are dropped too, expired or not. This loses data on purpose, as RocksDB's FIFO `max_table_files_size` does: a tombstone dropped this way can uncover an older entry in an SST kept. The public crate does not expose it.
- **Bounded L0 without a TTL or cap.** FIFO keeps flushes in L0. Once `max(l0_trigger, 2)` adjacent L0 files fit in `target_sst_bytes` together, the longest such window merges into one L0 file (RocksDB FIFO's intra-L0 compaction). The file count stays below `l0_trigger` per target-sized slice of the data, and each file still covers a short span of time, so expiry stays fine-grained. A family with neither TTL nor cap never drops anything.
- **Write stall.** For FIFO, `stall_score` is that window's length over its trigger, not the L0 file count, since FIFO's L0 is meant to be deep.
- **GC of an L0 output (engine).** `gc_policy` treated an output whose deeper levels are empty as bottommost. An L0 output is now bottommost only if every SST of the slot is an input, because L0 files left out of a window may be older. Purges stay correct, and `CompactionRecord`'s model purge matches.
- **Full compaction.** `Engine::compact` still merges a FIFO family into one last-level run, which then expires only when its newest entry does.
- **Clock.** Expiry uses the shard's wall clock, as reads do. A clock that steps backwards after a drop could make a read treat the dropped data as live again (it is gone). This is accepted, as for the TTL GC in rewrites.
