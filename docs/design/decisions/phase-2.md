# Decisions made in Phase 2 (D163–)

Indexed in [README.md](README.md). Numbers are permanent and continue from Phase 1; code and docs cite them as `Dn`.

<a id="d163"></a>
## D163 — Fairness rules for the Phase 2 benchmark against SQLite EAV and hand-keyed stores (approved; bench, #54, #220)
The Phase 2 gate compares Pigeonhole's sparse-wide workload with SQLite EAV and hand-keyed RocksDB/fjall. Four questions from adding timestamped puts and family reads:

### Q: Do the key-value runners pay for versions the way Pigeonhole does?
Phase 2's gate is "sparse-wide beats SQLite EAV and hand-keyed RocksDB". Pigeonhole's
families keep `max_versions(1)`, and every runner overwrites a cell in place, so no workload
reads or retains more than the latest version. A hand-keyed RocksDB or fjall store that
supported versions would put an inverted timestamp in the key and scan to the newest, which
costs more than the overwrite the runners do now; SQLite EAV would add `ts` to the primary
key. Comparing a versioned Pigeonhole feature against engines that do not offer it is fair
only while nobody reads old versions.

**Interim behavior:** all engines keep the latest version only. Cells carry their timestamp
(Pigeonhole natively, the others as an 8-byte value prefix or a `ts` column), so the TTL work
is comparable. A versions workload needs a `BenchOp` that reads `n` versions and a keyed
layout in each comparison runner; defer until a Phase 2 gate names one.

### Q: Is a read-time TTL filter a fair stand-in for Pigeonhole's compaction-time expiry?
Pigeonhole drops expired cells during compaction and filters them on read. The comparison
runners only filter on read and never delete an expired cell, so their stores grow and their
scans step over dead cells, which Pigeonhole's compactions eventually remove. RocksDB has a
TTL compaction filter and fjall has none; a hand-written layout would want one. The first
effect favors Pigeonhole on space and on long runs; the second favors the others on write
cost.

**Interim behavior:** read-time filtering only, in every engine, so all of them return the
same cells (checked by the agreement tests). Compare store size and scan latency of
`time-series-ttl` with that in mind. A RocksDB compaction filter is the first thing to add if
the numbers look lopsided.

### Q: Event times and the wall clock
TTL is judged against the wall clock when a read runs, but the generator is deterministic
from the seed. `WorkloadConfig::epoch_micros` (0: wall clock at `Workload::new`) anchors event
times; loaded points sit at least about 9.6 minutes from the expiry boundary on either side,
so engines agree on which are live unless a run lasts that long between workload creation and
a read. A quarter of the loaded points are expired.

**Interim behavior:** as above. Runs where load plus measurement exceed ten minutes (`full`
scale on a slow disk) can see engines disagree at the boundary; the report does not detect
that. Consider re-anchoring `epoch_micros` after the load phase if it matters.

### Q: FIFO-by-time compaction
The issue asks to add it "once Phase 2 ships it". `pigeonhole::Compaction::FifoByTime` is in
the public API, but the compaction picker for it is still a stub (`picker.rs` ignores `now`
and the TTL), so selecting it would change nothing the bench could measure.

**Interim behavior:** the `metric` family uses the default leveled compaction. Switch the
runner to `FifoByTime` when the picker lands; the hand-written engines have no equivalent
(RocksDB has `FIFO` compaction with a TTL, which is the fair counterpart to add then).

**Coordinator:** confirmed, all four as interim:
1. **Versions:** every engine keeps the latest version only; the sparse-wide gate workload never reads old versions, so the comparison is fair. Versions are validated by Pigeonhole's own correctness tests, not by the gate bench.
2. **TTL:** read-time filtering in every engine, so all return the same cells; report store size next to `time-series-ttl` numbers. If they look lopsided, add a RocksDB TTL compaction filter first.
3. **Event times:** as described; re-anchoring after the load phase is #222, to land before the gate benchmark runs.
4. **FIFO-by-time:** leveled until the picker lands (#32); then switch the `metric` family and give RocksDB its FIFO-with-TTL compaction as the counterpart.

<a id="d164"></a>
## D164 — The engine's test hooks live in one module, and a public seam beats a hook (approved; engine, #184, #216–#218; touches D90, D134)
Phase 1 left about a hundred `#[cfg(feature = "test-hooks")]` sites spread over `engine.rs`, `shard.rs`, `manifest.rs`, `maintenance.rs` and `snapshot.rs`. Issue #184 moved them into `crates/engine/src/engine/hooks.rs`: the `#[doc(hidden)]` `Engine` hook methods, the types they return, and the state they keep (`Shared::hooks`, `ShardMetrics::hooks`, `ReaderState::hooks`). The module docs list the rules: every hook has a committed test that uses it, a hook does nothing until a test sets it, and a public or application-owned seam is preferred to a new hook.

D90 and D134 name hooks individually. D134's `tablet_changes` hook is gone: tests sum `Engine::shard_stats` instead, the per-shard counters the bench added for #51.

**Interim behavior:** as described; the `test-hooks` feature and every remaining hook behave as before.

**Follow-up question (recording in `test-hooks` builds):** `take_appended` and `take_compactions` read back records that every `test-hooks` build keeps (every WAL record appended, every rewrite compaction), and the shard counters are stored after every batch. Because of workspace feature unification (`cargo test --workspace --all-features`, #148 1-2 F10), the public crate's suites run against such an engine too, so those two vectors grow for the length of a run that never drains them. Nothing reads them there, so results do not change, but memory use does. Making recording opt-in (a test turns it on before it reads it) would change the harness, so it is left to #148.

**Interim behavior:** recording stays on in every `test-hooks` build.

**Coordinator:** confirmed. On the follow-up: make the recording opt-in, turned on by the tests that read it, so suites built with the feature through workspace unification don't grow those vectors; tracked with the unification fix in #148.

<a id="d165"></a>
## D165 — The tiered picker's runs, triggers and output levels; the engine picks per family (approved; compaction, #31, #227)
The spec says only "tiered/universal for write-heavy families". `Levels` keeps L0 overlapping and newest first and every deeper level sorted and disjoint, so a merged run must land in a whole level.

**Interim behavior:**
- The sorted runs are each L0 file and each non-empty deeper level, newest first.
- Once L0 holds `l0_trigger` files, all of them merge, taking in the following level runs while each is at most `PickerOptions::tiered_size_ratio_percent` (default 1) larger than what was taken so far. Level 1 is always taken when it is not empty, since the output must go above the first run left out.
- Once the runs above the oldest one hold more than `PickerOptions::tiered_max_space_amp_percent` (default 200) of its bytes, every run merges into the last level.
- The output goes just above the first run not taken (the deepest free level), or to the last level when every run was taken. Runs therefore stay ordered newest first down the levels, so GC's `bottommost` and `min_ts_above` mean what they do for leveled. Whole runs move, so no row is ever split (D78 holds trivially).
- A lone L0 file over levels it does not need to merge with is a `TrivialMove`.
- `score`, which drives picking, is the larger of L0 depth over `l0_trigger` and space amplification over its cap.
- The two `PickerOptions` fields are additive (`PickerOptions` is `#[non_exhaustive]`), engine-wide like the leveled knobs; the public crate exposes none of them.

**Per-family picking:** The engine built one leveled `CompactionPicker` per shard and used it for every family.

**Interim behavior:** each shard keeps a leveled and a tiered picker and scores and picks each `(tablet, family)` slot with its family's style (a `match`, so a new style fails to compile). `FifoByTime` families keep compacting leveled until #32's picker lands (engine-level only; the public crate refuses the style, D95). Full compactions (`Engine::compact`) and #95 cleanups still merge everything into the last level whatever the style. The engine model-check harness gives family `g` the tiered style, so every suite and seed sweep runs both pickers against the oracle; runs that turn background compaction off for deterministic purges also set `tiered_max_space_amp_percent = u32::MAX`.

**Follow-up question (write amplification):** Because L1 is always taken when it holds a run, every L0 merge after that rewrites all of L1. L1 grows by one L0 batch per merge until the size ratio takes in L2 or space amplification fires, and it can reach about 2× the last level first. Over k merges that rewrites up to k batches each, so write amplification grows roughly quadratically in k, where universal compaction's is logarithmic.

**Proposal:** accept this for now. Tiered is still opt-in, and the public crate refuses it until #44. Bound it in [#228](https://github.com/CodingAnarchy/pigeonhole/issues/228) (Phase 2). The preferred fix there is sorted runs in L0, as RocksDB universal does, which is a manifest/format change. The format-free alternative is to push L1..Lk down into a free level before L1 would be forced.

**Interim behavior:** as described; the picker proptest bounds run count and space amplification but not write amplification.

**Coordinator:** confirmed. On write amplification: accepted for now, but #241 made Tiered public, so bounding it (#228) must land before the next published release (0.2.0).

<a id="d166"></a>
## D166 — The write stall follows L0 depth only (approved; compaction, engine, #227; amends D119)
The engine set the stall score to the highest picking score among the shard's slots. That already let a leveled deeper level over its target pace writers. With tiered's space amplification included, a fresh tree with three equal L0 files (200% amplification, the default cap) would have stalled writers below `l0_trigger`, for the length of a full-tree merge.

**Interim behavior:** the added `CompactionPicker::stall_score` returns L0 depth over `l0_trigger` for every style, and the stall uses only that. `score` (L0, deeper levels, space amplification) only decides which slot compacts first. For leveled families this narrows the stall to L0, as D119 describes it.

**Coordinator:** confirmed.

<a id="d167"></a>
## D167 — The FIFO-by-time picker: expiry drops, the size cap and intra-L0 merges (approved; compaction, #32, #229)
The spec says "whole SSTs drop when their newest timestamp expires, with no rewrite". Issue #32 also asks for a size-based fallback and for a decision on tombstones in dropped SSTs.

**Interim behavior:**
- **Expiry.** `pick(.., now, ttl_micros)` returns one `TaskKind::Drop` of every SST, at any level, whose newest timestamp has expired (`ts_range.1 + ttl <= now`, the model's TTL rule), unless it is busy. A drop has no job and no I/O. Reads at every snapshot are unchanged from `now` on: every entry in the SST has expired, and so has everything one of its tombstones hides, since a delete at `T` hides only timestamps `<= T` (D74). A dropped tombstone therefore never uncovers live data. No extra rule is needed, and the drop is not a purge, so no `CompactionRecord` is kept.
- **Time-aware score.** The added `CompactionPicker::score_at(levels, now, ttl_micros)` scores at least 1.0 exactly when `pick` at the same `now` has work: an expired SST exists, the bytes exceed the cap (not merely reach it), or a window is due. The one exception is work whose SSTs are busy, which the score cannot see, as for every style; the engine moves on to the next due slot. Retrying a busy window, and a timer for expiry on idle families, are #232. `score(levels)` is `score_at(levels, 0, 0)`. The engine calls `score_at` with the shard clock and the family's TTL. Expiry is noticed only when the slot's maintenance runs (after a manifest commit), not by a timer: an idle family keeps its expired SSTs, invisible to reads, until its next flush or compaction.
- **Size cap.** The added `PickerOptions::fifo_max_bytes` (default 0: none, engine-wide). Past it, the SSTs with the oldest newest timestamps are dropped too, expired or not. This loses data on purpose, as RocksDB's FIFO `max_table_files_size` does. With explicit timestamps it can also change what remains in ways a reader can see:
  - A dropped tombstone can uncover an older entry in an SST that is kept.
  - An older version can come back. Take SST A with `put(c, ts=100)` and SST B with `put_at(c, ts=50)` and `put(x, ts=200)`. A's newest timestamp (100) is older than B's (200), so the cap drops A first, and reads of `c` then return the ts-50 value.
  - Dropping an SST that holds a counter's base or some of its operands makes the counter go backwards.

  None of this happens without explicit timestamps, since time-ordered writes put older versions in SSTs with older newest timestamps. The cap is off by default and the public crate does not expose it.
- **Bounded L0 without a TTL or cap.** FIFO keeps flushes in L0. Once `max(l0_trigger, 2)` adjacent L0 files fit in `target_sst_bytes` together, the longest such window merges into one L0 file (RocksDB FIFO's intra-L0 compaction). The file count stays below `l0_trigger` per target-sized slice of the data, and each file still covers a short span of time, so expiry stays fine-grained. A family with neither TTL nor cap never drops anything.
- **Write stall.** For FIFO, `stall_score` is that window's length over its trigger, not the L0 file count, since FIFO's L0 is meant to be deep.
- **GC of an L0 output (engine).** `gc_policy` treated an output whose deeper levels are empty as bottommost. An L0 output is now bottommost only if every SST of the slot is an input, because L0 files left out of a window may be older. Purges stay correct, and `CompactionRecord`'s model purge matches.
- **Full compaction.** `Engine::compact` still merges a FIFO family into one last-level run, which then expires only when its newest entry does.
- **Clock.** Expiry uses the shard's wall clock, as reads do. A clock that steps backwards after a drop could make a read treat the dropped data as live again (it is gone). This is accepted, as for the TTL GC in rewrites.

**Coordinator:** confirmed. Follow-ups: an expiry timer and busy-window retry (#232); blob pointers in dropped SSTs are accounted in the blob separation PR (#235).

<a id="d168"></a>
## D168 — Tiered and FifoByTime families are accepted by the public API (approved; pigeonhole, #44, #241; amends D95)
D95 refused `Compaction::Tiered` and `Compaction::FifoByTime` at table creation until their pickers existed. They now do (#31, #32).

**Interim behavior:**
- Both styles are accepted and stored. `Family::zstd` is still refused with `Unsupported` until the codec lands; the rest of #44 stays open for it.
- `FifoByTime` without a TTL is accepted, not refused, although the guide used to say it "needs a TTL". Nothing expires then, and small files still merge, so it is merely pointless. The docs say so.
- The public model test gives family `g` the tiered style and `ttl` the FIFO style, so its sweeps cover both pickers through the public API.
- The engine-wide tuning (`PickerOptions::tiered_*`, `fifo_max_bytes`) is not exposed.

**Coordinator:** confirmed; `zstd` stays refused until its codec lands (#44 stays open).

<a id="d169"></a>
## D169 — The tiered picker needs no extra write-amplification bound; the proptest guards it (approved; compaction, #228, #245; supersedes D165's write-amp follow-up)
#228 was filed from a review of #227. Its concern: since L1 is always taken when it holds a run, every L0 merge would then rewrite a growing L1, so write amplification would grow about quadratically. D165 makes a fix required before the next release.

**Finding:** the picker never reaches that state while a free level exists. An L0 merge's output goes just above the first run it leaves out, which is the *deepest* free level. Runs therefore fill the levels from the bottom up, and L1 holds a run only once every level 1..last does. From then on some merge of existing runs is unavoidable, and the size-ratio cascade (L0 + L1, then L2 once L1 has grown to its size, and so on) behaves like a size-tiered scheme with `max_levels - 1` runs below L0. Measured with insert-only data (no GC shrinkage), 1 MiB flushes, `l0_trigger` 4 and the default 1% ratio:

| max_levels | flushes | current picker | format-free fix (shift runs down, else merge the most similar adjacent pair) |
|---|---|---|---|
| 7 | 1000 | 4.1× | 5.2× |
| 7 | 4000 | 5.8× | 8.8× |
| 7 (space-amp cap 1000×) | 4000 | 5.8× | 8.8× |
| 3 | 1000 | 42× | 63× |
| 3 | 4000 | 165× | 251× |

At the default 7 levels growth is logarithmic, about +1.7× per 4× more data. With 3 levels (2 runs below L0) it grows linearly in the data for both pickers, as any scheme with two runs must above O(√N). The format-free fix was worse everywhere, because it rewrites deep runs that the cascade leaves alone. A shift-only variant (push runs down into a free level, else take L1 as now) never fires, for the reason above, and matches the current picker exactly.

**Proposal:** keep the picker. Guard against a regression with a write-amplification bound in `tiered_picker_keeps_runs_and_space_amp_within_bounds`, run with insert-only merges: `levels × batches^(1/(levels-2)) + 2`, where `batches` is flushes over `l0_trigger`. A picker that rewrote a growing L1 into every L0 merge breaks it: a hacked picker measured 75.9× against a bound of 28.6 at 5 levels. Close #228. If shallow tiered families (`max_levels` 3–4) matter, sorted runs in L0 (a format change) is the real lever; that is a separate issue, if wanted.

**Interim behavior:** the picker is unchanged; only the test gained the bound.

**Coordinator:** confirmed. #228 is closed by #245: no picker change, a write-amplification bound in the picker proptest. Shallow tiered families (`max_levels` 3) grow write amplification linearly; the guide should say so (docs follow-up).
