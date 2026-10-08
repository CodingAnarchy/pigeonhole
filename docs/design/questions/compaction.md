# Compaction questions (Phase 2)

## Proposed decision: the tiered picker's runs, triggers and output levels (#31)
The spec says only "tiered/universal for write-heavy families". `Levels` keeps L0 overlapping and newest first and every deeper level sorted and disjoint, so a merged run must land in a whole level.

**Interim behavior:**
- The sorted runs are each L0 file and each non-empty deeper level, newest first.
- Once L0 holds `l0_trigger` files, all of them merge, taking in the following level runs while each is at most `PickerOptions::tiered_size_ratio_percent` (default 1) larger than what was taken so far. Level 1 is always taken when it is not empty, since the output must go above the first run left out.
- Once the runs above the oldest one hold more than `PickerOptions::tiered_max_space_amp_percent` (default 200) of its bytes, every run merges into the last level.
- The output goes just above the first run not taken (the deepest free level), or to the last level when every run was taken. Runs therefore stay ordered newest first down the levels, so GC's `bottommost` and `min_ts_above` mean what they do for leveled. Whole runs move, so no row is ever split (D78 holds trivially).
- A lone L0 file over levels it does not need to merge with is a `TrivialMove`.
- `score` is the larger of L0 depth over `l0_trigger` and space amplification over its cap, so the L0 write stall (D119) paces writers as it does for leveled and ends once a compaction lands.
- The two `PickerOptions` fields are additive (`PickerOptions` is `#[non_exhaustive]`), engine-wide like the leveled knobs; the public crate exposes none of them.

## Proposed decision: the engine picks per family (#31)
The engine built one leveled `CompactionPicker` per shard and used it for every family.

**Interim behavior:** each shard keeps one picker per `CompactionStyle` and scores and picks each `(tablet, family)` slot with its family's style. Full compactions (`Engine::compact`) and #95 cleanups still merge everything into the last level whatever the style. The engine model-check harness gives family `g` the tiered style, so every suite and seed sweep runs both pickers against the oracle; runs that turn background compaction off for deterministic purges also set `tiered_max_space_amp_percent = u32::MAX`.
