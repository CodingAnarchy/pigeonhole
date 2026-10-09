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

<a id="d170"></a>
## D170 — FIFO-by-time expiry runs on a timer; a busy L0 file splits the merge windows (approved; compaction, engine, #232, #246; refines D167)
D167 noticed expiry only when a slot's maintenance ran (after a flush or compaction), so an idle FIFO family kept expired SSTs, invisible to reads, and their space until its next flush. Its window merge also gave up when the single longest L0 window held a busy file.

**Interim behavior:**
- **Expiry timer.** The added `CompactionPicker::next_expiry(levels, ttl_micros)` returns the earliest `ts_range.1 + ttl` among a FIFO-by-time family's SSTs (`None` for other styles or without a TTL). `maintain` takes the earliest still in the future across the shard's slots and arms the compaction retry timer for it (`arm_compaction_retry`, converting the wall-clock wait to monotonic nanoseconds). When it fires, `maintain` runs and drops what expired. It shares the backoff timer, which keeps the earlier deadline.
- **Stopped clock.** The timer is not armed while the existing one gave up on a stopped clock that still reads the same (D126, D161), so a frozen simulator clock cannot make the shard spin. There, expiry falls back to the next flush or compaction, as before.
- **Busy windows.** A busy L0 file now splits the windows `pick` considers, and the longest window without one merges. `score_at` still ignores busy files (D167's one exception).

**Coordinator:** confirmed. `arm_compaction_retry` keeps the earliest deadline, so an expiry never postpones a slot backoff (tests on #246); idle_cpu tests cover a FIFO family with a far-future expiry.

<a id="d171"></a>
## D171 — A room wait's re-check timer can tell the clock stopped (approved; engine, #244, #252; refines D126, D161)
D126 refuses a commit waiting for arena room with `Busy` on a frozen clock once nothing in flight can free room, since a stopped clock never reaches the stall timeout. "Frozen" was read only from the wait's timeout timer, which gives up after polling an unchanged clock.

The public model suite (`quiet_runs_with_tablet_changes_off_match_the_model`, seed 53, on CI in a 50-seed process) hung 7 times in 20 runs. The harness advances the simulated clock from its own thread, then blocks in a `GroupSync` commit at op 391 whose room snapshots hold. The CI trace (via the new watchdog) shows the mechanism:
1. The timeout timer saw the clock move while the wait began, and slept towards its 30 s deadline, which the stopped clock never reaches.
2. Every ~3 ms the 1 ms re-check timer (D161) gave up on the stopped clock and kicked the shard, which retried, found no room and armed a new re-check.
3. The shard therefore never went idle, so the sleeping timeout timer was never polled again to notice, and the commit never resolved.

**Interim behavior:**
- `RoomWait::stopped(now)`: the clock stopped if either the timeout timer or the re-check timer gave up on this reading. The room wait and the starved-freeze wait both use it, so the case above takes D126's frozen-clock path (refused with `Busy`) at the first re-check that gives up.
- On a moving clock nothing changes, because a re-check timer never gives up there (`a_reader_pin_waits_for_the_stall_timeout_on_a_moving_clock` still waits for the timeout).
- After the fix: 12 of 12 runs of the failing chunk passed. A deterministic end-to-end reproduction needs engine-owned threads with a clock stepped from another thread; the app-owned runtime re-polls sleeping timers whenever it idles, so it never reaches this state. The regression test is therefore the unit test `a_room_wait_sees_a_stopped_clock_through_its_recheck_timer`, plus the CI repeat runs.
- Not new in the tiered/FIFO work as such. 0.1.0 passed 4 of 4 runs of the same chunk, but this seed's timing reached the state only once the public suite's families changed with #241.

**Coordinator:** confirmed. The public `model` suite now has a per-seed watchdog (`PIGEONHOLE_SEED_TIMEOUT`, default 120 s) that names a hung seed and aborts.

<a id="d172"></a>
## D172 — Registered merge operators reach the engine; unregistered ones make the handle read-only (approved; pigeonhole, engine, #43, #253; supersedes D102)
D102 kept `Options::merge_operator` / `ReaderOptions::merge_operator` registrations without passing them on, so any family naming an operator other than `pigeonhole.i64_add` was refused.

**Interim behavior:**
- Both option types register every operator in `EngineOptions::merge_operators`. The engine already resolved registered names (`MergeKind::Registered`) for reads and compaction. Registering one under `pigeonhole.i64_add` replaces the built-in, as `MergeRegistry::register` does.
- A family naming an unregistered operator is still refused at creation with `UnknownMergeOperator`.
- **Read-only, as documented.** `allow_unregistered_merge_operators(true)` promised a read-only handle with compaction off. The engine skipped compaction of those families but still accepted writes, catalog changes and transactions. A writer opened over a family whose operator is unknown now refuses them with `ReadOnly`, as a reader process does. `flush`, `compact` (which skips those families) and `close` still work.
- The operator sees stored values (a tag byte, then the payload), as the built-in does. The guide says so.
- A merge onto a base stored in a blob file passes the operator the blob pointer, not the value. That is #235's fix (blob separation); custom operators inherit it when it lands.

**Coordinator:** confirmed. Merging onto a base stored in a blob file is fixed with blob separation (#235), which custom operators inherit.

<a id="d173"></a>
## D173 — FUSE and GPFS count as network filesystems; the local check runs before the writer lock (approved; io, engine, #147, #258; refines D37)
The review of #90 (3-4 4.3) found two gaps in network-filesystem detection.
- On Linux, `is_local` did not list FUSE or GPFS. Locks on sshfs, s3fs or gcsfuse may be local to one host, so two hosts could both be writers.
- The writer took its lock before the check. On NFS without lockd (or mounted `nolock`), `F_OFD_SETLK` failed with `ENOLCK`, and the open reported `Io("lock")` instead of `NetworkFilesystem`.

**Interim behavior:**
- Linux treats FUSE (`0x65735546`) and GPFS (`0x47504653`) as non-local. That includes local FUSE filesystems such as ntfs-3g, which are refused until an explicit opt-in exists. None is planned; it is a follow-up if anyone asks.
- `Engine::open` checks `is_local` (a read-only `fstatfs`) before `WriterLock::acquire`. D37 orders the presence lock and shared memory, not this check. Reader processes already checked first.
- Regression test: `engine/tests/network_fs.rs` uses a VFS whose files are remote and whose locks fail like `ENOLCK`.

**Coordinator:** confirmed as the safe default. Refusing *local* FUSE filesystems too (ntfs-3g, encrypted home directories such as gocryptfs) is flagged to the owner; an opt-in for trusted local FUSE mounts is the likely follow-up if they want it.

**Owner:** keep refusing FUSE and GPFS by default, and add an explicit opt-in for trusted local FUSE mounts only. Network filesystems stay refused with the opt-in set (#299).

<a id="d174"></a>
## D174 — Test-hook recording is opt-in, and the test-hooks wait matches production (approved; engine, #148, #261; refines D164)
**Interim behavior:**
- **Opt-in recording.** `take_appended` and `take_compactions` record only after `Engine::record_history(true)`; recording is off at open. Compaction records are not even built while it is off. Because of workspace feature unification, every `test-hooks` build (the public crate's suites included) used to grow both vectors for a whole run nobody read. The tests that read them turn recording on: the model harness at each open, the tablets helpers, and the few direct readers. The `ShardCounters` stay always-on plain counters.
- **One wait semantics (F10).** The `test-hooks` `PendingMaintenance` future used to resolve at the first failed shard reply, while other shards still worked. It now resolves only once every shard has replied, with the failure if there was one, as production's blocking `wait` does. The harness therefore never sees a `flush`/`compact` result production cannot produce.
- **Production waits under test.** CI's Linux job also runs `cargo test -p pigeonhole --all-features` on its own. That run builds the engine without `test-hooks` (feature unification adds it only for the workspace run), so the public suites cover the production wait path too.

**Coordinator:** confirmed. CI's Linux job also runs the public crate's tests on their own, so the production (non-test-hooks) engine build is exercised.

<a id="d175"></a>
## D175 — The zstd codec, its library and its level (approved; format, sst, pigeonhole, #44, #255; amends D168)
FORMAT.md already reserved codec 2 for zstd and `FamilyOptions::compression_level` (i8, default 3). D168 kept `Family::zstd` refused until a codec existed.

**Interim behavior:**
- **Encoding.** A zstd block's payload is one standard zstd frame (content size included, no dictionary), written at the family's `compression_level` with libzstd's meaning: 1–22, negative for faster, 0 for its default, out-of-range values clamped. As for LZ4, a block stays uncompressed when zstd saves less than 1/8. Decoding goes into a buffer of exactly `uncompressed_len` bytes, so a frame that would decode to more or less is `Corrupt`. The level is a writer setting and is not stored per block. FORMAT.md §4.1 says so.
- **API (additive).** `compress::compress_with_level`, `block::seal_with_level` and `compress::DEFAULT_ZSTD_LEVEL` are new. `compress` and `seal` keep their signatures and use level 3. `SstWriterOptions::compression_level` (the struct is `#[non_exhaustive]`) is set by `for_family`.
- **Library: the `zstd` crate (libzstd via `zstd-sys`; MIT/BSD-3; `cargo deny` passes), not the pure-Rust `ruzstd`.** `ruzstd` 0.9's encoder implements only its fastest level (about zstd level 1), so `zstd(level)` would mean nothing. libzstd gives every level, faster decoding on the read path, and the trained dictionaries the spec mentions for later. Costs: a C toolchain at build time (via `cc`, already in the tree), and Miri cannot run the codec. The format and SST tests skip zstd under `cfg(miri)`, and CI's Miri job does not cover these crates anyway. The FFI's `unsafe` stays inside that crate, so `pigeonhole-format` keeps `#![forbid(unsafe_code)]`.
- **Public crate.** `Family::zstd(level)` is accepted, so `Family::to_engine` refuses nothing and is now infallible. The engine and public model harnesses store family `f` with zstd, so the seed sweeps cover it.
- **Not yet.** Trained dictionaries: FORMAT.md's reserved "compression dictionary address" stays absent.

**Coordinator:** confirmed as interim. The C dependency (libzstd via `zstd-sys`) is flagged to the owner; if they prefer a pure-Rust tree, revisit when `ruzstd` gains real compression levels.

**Owner:** keep libzstd (the C dependency). This decision is final, not interim.

<a id="d176"></a>
## D176 — The stopped-clock fallbacks apply only to a simulated clock (approved; io, engine, runtime, #263, #267, ICR 0012; amends D126, refines D161, D171)
D126's fallbacks for a clock that does not move (admit writers when no compaction can end an L0 stall, refuse a hopeless room wait at once, cap flush retries) were triggered by a poll count. A timer that read the same clock value 1024 times gave up. A coarse real clock reads the same for milliseconds and got the simulator's fallbacks on a moving clock (review 1-2 F7).

**Interim behavior (ICR 0012):**
- The added provided method `Vfs::clock_is_simulated()` is `false` by default and `true` for `SimVfs`.
- On a real clock, `ClockTimer` sleeps to its deadline and never gives up, and the runtime never wakes sleepers early on matching readings. Waits end by their timers and timeouts, as D124 and D161 describe for a moving clock.
- On a simulated clock nothing changes: the poll count still detects a stopped clock, and D126, D161 and D171 apply as before.
- Test wrappers around `SimVfs` forward the method; wrappers that substitute a real clock keep the default.
- Regression test: `engine/tests/coarse_clock.rs` uses a real clock in 4 ms steps. A commit waiting for room that snapshots hold gets `Busy` only after the 300 ms stall timeout; with the old behavior it was refused after 1.6 ms.

**Coordinator:** confirmed; ICR 0012 (`Vfs::clock_is_simulated`, an additive provided method) approved on #267.

<a id="d177"></a>
## D177 — Backup releases its snapshot's memtables before the long merge (approved; engine, #262, #268)
`backup` held one snapshot, memtables included, for its whole run. A snapshot keeps its memtables' arena chunks allocated, so a backup of a large file (minutes) left writers to fill the rest of the arena and fail with `Busy` at the stall timeout (D124, D138). On a simulated clock the hopeless case was refused at once.

The options weighed:
- **Copy the memtables first, then keep only the SST view** (chosen). The snapshot point stays exactly the call time, as documented. The memtable copy is bounded by the arena: it reads memory, but it writes (and compresses, LZ4 or zstd per family) up to an arena's worth of SSTs to the new file while the memtables are pinned, so its length is that write's. The long part reads SSTs whose extents the kept view pins.
- **`flush()` first, then back up an SST-only snapshot.** This is simpler, but the snapshot would still include memtables with writes that land between the flush and the snapshot. It also turns every backup into a forced flush of every slot (more L0 files, a compaction burst) and moves the point in time.

**Interim behavior:**
- **Phase 1.** For every slot, the snapshot's memtable entries at or below its seqno go into temporary SSTs in the new file.
- **Release.** `backup` then builds a view with the same tablets, catalog and SST set but no memtables, registered (`ViewPin`) so its SSTs stay unreclaimed, and drops the snapshot. Its memtables' chunks are freed as soon as nothing else holds them. The seqno pin goes too: compactions may garbage-collect past the snapshot meanwhile, which is harmless since the backup reads the pinned old SST files.
- **Phase 2.** Each slot merges its source SSTs with its temporary SSTs and writes the final last-level SSTs. The temporary extents are abandoned in the new file right after (the new file's bitmap is rebuilt from its manifest at open anyway, D8). Temporary SSTs are read through a private block cache, because the new file's SST ids start at 1 and would collide with the engine's cache keys.
- **Cost.** The new file briefly holds up to one arena's worth of temporary SSTs. Their abandoned extents become free space that the phase-2 outputs reuse only in part (they are allocated in other size classes and order), so the copy can end up to about an arena larger than a freshly compacted file. It is still a valid file: free extents are not persisted, and its bitmap is rebuilt from the manifest at open (D8); `shrink` reclaims the tail. The old SSTs a compaction replaces during the backup stay allocated until it ends, as they did.
- **Test hook.** `Engine::after_backup_releases_memtables` runs between the phases. No public seam can observe the release, and the test (`backup_releases_the_memtable_arena_before_its_long_merge`) checks that a flush there gives the arena back.

**Coordinator:** confirmed. With blob separation (#235/#238), phase 1 writes values inline and only phase 2 separates (from the #268 review).

<a id="d178"></a>
## D178 — The bench `metric` family compacts FIFO in Pigeonhole and RocksDB; RocksDB scans merge column families only once `metric` is written (approved; bench, #236, #270; refines D163)
D163 asked to switch Pigeonhole's `metric` family to `FifoByTime` once the picker existed, and to give RocksDB FIFO compaction with a TTL as the fair counterpart. RocksDB's FIFO is per column family, the runner kept every family in one, and `BenchOp::Scan` carries no family. So the question was how a scan finds `metric` cells without slowing the other workloads' scans, the sparse-wide gate's included.

The options weighed:
- **Route by key prefix** (`ts:` rows live only in `metric`). This is cheapest, but the runner's correctness would hang on the workload's key layout.
- **A per-run workload hook** opening the default column family as FIFO for `time-series-ttl`. This needs an ICR on `Runner`, and it would compact `metric`'s neighbours FIFO in a mixed run.
- **Merge both column families in every scan.** This is always correct, but adds a seek on an empty column family to every scan of every workload.
- **Merge only once `metric` was written** (chosen).

**Interim behavior:**
- Pigeonhole's `metric` family is `ttl(1 day)` plus `Compaction::FifoByTime`.
- RocksDB keeps `metric` cells in a `metric` column family with `DBCompactionStyle::Fifo`, `set_ttl(1 day)` and no size cap (`max_table_files_size = u64::MAX`, as Pigeonhole's FIFO has none). The other families stay in the default column family.
- RocksDB routes Put, PutAt, Get and GetRow by family. A scan merges the two column families' iterators in key order (keys never collide, since the family byte is part of the key), but only once the runner has written a `metric` cell. A run that never writes one, every workload but `time-series-ttl`, scans exactly as before.
- Caveat for comparisons: RocksDB's FIFO TTL counts from a file's creation, not from the cells' event times, so it drops the loaded back-dated points later than Pigeonhole does. Within one run (well under a day) it drops nothing. `docs/bench.md` says so next to the store-size note. SQLite and fjall have no FIFO and keep filtering on read.
- The agreement tests (RocksDB, SQLite and fjall against Pigeonhole on every workload) pass: every engine reads the same cells.

**Coordinator:** confirmed. The sparse-wide gate workload scans one RocksDB iterator, unchanged.

<a id="d179"></a>
## D179 — Counters are declared families, combined per timestamp like Bigtable aggregates (approved; owner decision 2026-10-08; pigeonhole, engine, compaction, #274; supersedes the cross-timestamp fold proposed in #233/#34)
Counters used to be any column written with `incr`: every family defaulted to the `pigeonhole.i64_add` operator, and each increment carried its commit timestamp. Operands combine only within one (column, timestamp) (D73), so a counter kept one operand per increment for ever. #233 proposed folding operands across timestamps at bottommost compactions; that changes what later explicit-timestamp writes (`put_at`, `delete_cell`) mean, and a write-time guard could not be scoped, because every family was a merge family.

**Decision (owner):** follow Bigtable's aggregate column families.
- A family is **declared** a counter family (Sum over i64 first; Min, Max and HyperLogLog may follow). Ordinary families have no merge operator by default, and `incr` on them fails with `InvalidArgument`. Families naming a custom operator (D172) are unchanged.
- In a counter family `incr` writes its operand at a **fixed timestamp** (0); `incr_at` writes at a caller-chosen bucket timestamp (for hourly or daily totals). Operands at the same (column, timestamp) combine at read time and in compaction under D73, so a counter column holds one cell per bucket, and no compaction ever changes the meaning of a later write.
- `put_i64` sets a bucket (later increments add to it); deletes work as for any cell; non-i64 puts into a counter family are refused. `max_versions` and TTL apply per bucket.
- Families written by 0.1.0 (which store `pigeonhole.i64_add` and may hold operands at commit timestamps) keep opening and reading correctly; their classification and a migration note are part of #274.

**Coordinator:** recorded from the owner's decision; #233 closed, #34 superseded.

<a id="d180"></a>
## D180 — Blob separation and blob GC: what is separated, how it is accounted and collected (approved; compaction, engine, #33, #235; amends D29, D77)
### Proposed decision: what a blob record holds, and which values are separated
FORMAT §7 left open whether a blob record holds the value's payload or its stored form, and §3 says the pointer's `len` is the "value length". The spec says values "above a per-family threshold" are separated.

**Interim behavior:** a blob record holds the stored value the pointer replaced, tag byte included, and the pointer's `len` is that stored length; a read returns the record's bytes as the stored value, pinned in the block cache, with no copy and no re-tagging. A put is separated when its stored value has tag `Bytes` and its payload is longer than `blob_threshold` (`u32::MAX` never; `separates`). Typed values (at most nine bytes) and merge operands are never separated, so an `i64` counter's base is never a pointer whatever the threshold. FORMAT §7 says so.

### Proposed decision: separation happens at flush as well as at compaction
Issue #33 says "at the output level". With separation only in compaction outputs, a lone L0 SST that the picker trivially moves down keeps its large values inline indefinitely (sequential loads do this), and every value is written once into an SST before it reaches a blob file, while the spec says large values are "written once to a blob extent, so compaction never rewrites them".

**Interim behavior:** the engine's flush task separates (through the same `BlobSink` and `separates` rule), and every compaction separates whatever is still inline (values flushed before a threshold change, values written by an open-time spill, which stays inline). The flush commits `PutBlobFile` edits with its `AddSst`s; a flush for a table dropped meanwhile frees its blob files with its SSTs.

### Proposed decision: the engine drops blob files; `dropped_blob_files` stays empty
D80 already said the job cannot know a file's live bytes. After a split both children reference the parent's SSTs and therefore its blob files, so no single job can tell that a file is empty.

**Interim behavior:** `CompactionOutput::dropped_blob_files` is always empty. The job reports `blob_live_delta`: `-(16 + len)` for every pointer it drops (D80) and for every value a blob GC copies out of a file it empties. The engine applies the deltas to the catalog **at commit time** (a `ReqKind::Catalog` request, since other tablets' compactions change the same files) and emits `DropBlobFile` for a file whose count reaches zero. A delta larger than the recorded count (an undercount somewhere) is refused with `Corruption`: the request commits nothing and its outputs are freed, instead of dropping a file some SST may still point into. Invariant (checked by the `check_blob_accounting` test hook at every full dump of the engine model harness): a file's live bytes equal `16 + len` summed over the pointers the SSTs hold within their tablets' rows, an SST shared after a split counting once per tablet.

### Proposed decision: how blob GC picks work
Issue #33 asks to pick blob GC "from per-file live-byte ratios". The manifest does not record which SSTs point into which blob file.

**Interim behavior:** a file is a candidate when it is at least half garbage and holds at least `target_sst_bytes / 16` garbage bytes (`pick_blob_gc`). A `TaskKind::BlobGc` task rewrites **every** SST of one `(tablet, family)` into the last level, copying the values still in its candidate files into new files. Each shard remembers in memory which slots it has emptied each candidate from and does not pick them again for that file (a slot that committed a blob GC holds no pointer into the file afterwards), so the work terminates even if some count were off; after a reopen, a merge or a move a slot may be rewritten once more for nothing. Recording each SST's referenced blob files, so that blob GC rewrites only the slots that point into a file and termination survives a reopen, is #240. Blob GC runs only when no other compaction is due. `Engine::compact` turns each full-compaction rewrite into a blob GC of every file of the family with any garbage, so `compact()` reclaims what it can. No knob yet; a public one can come with the Phase 2 options work.

### Proposed decision: blob records are not compressed
zstd block compression (#44, #255) compresses SST blocks with the family's codec.

**Interim behavior:** blob records hold the stored value as is, whatever the family's codec. The SST blocks that hold the 16-byte pointers are compressed as usual, so nothing is compressed twice, and a read of a separated value needs no decompression (it is pinned straight from the block cache). Families with compressible large values can raise `blob_threshold` to keep them inline and compressed. Per-record compression would need a codec byte in the record header (a format change); not planned.

### Proposed decision: blob extents are at most 1 MiB unless a value needs more
All extents of a blob file share one size class (FORMAT §7) and cannot be trimmed one by one, so the last extent's unused tail is wasted.

**Interim behavior:** `BlobSink` takes extents of about half the input (or memtable) size, capped at 1 MiB, and cuts files near `4 × target_sst_bytes`. A value more than four extents long starts a new file with extents of a quarter of its size (up to 64 MiB). A file of a single extent is trimmed to its length at finish (`Pager::trim`, D128).

### Proposed decision (amends D77): value predicates see separated values
D77 said a blob pointer matches no byte predicate because the resolver does not read blobs. With separation, a predicate's result would then depend on whether a compaction had run.

**Interim behavior:** additive `ResolveOptions::blobs: Option<Arc<dyn BlobFetch>>`. When it is set, the resolver tests a value predicate on the value a pointer names, and loads a separated merge base before folding operands onto it. Because the resolver's error type is its cursor's, `BlobFetch::fetch` returns `None` on failure and the implementation keeps the error: the engine reports it after each resolver step (`ResolverBlobs::check`), so a failed blob read is an error, never a silent non-match or a fold onto the pointer. On a version that names blob files, the engine sets the hook on every read path (gets, row reads, scans, `check_and_mutate`'s read, reader processes) when the read has a value predicate or the family's merge operator is not the built-in `pigeonhole.i64_add`. The built-in rejects every base that is not a stored `i64` with the same error, and a separated value never is one (only `Bytes` are separated), so loading the base could not change its result; skipping it keeps default families' reads free of a per-read allocation. `check_and_mutate` also resolves the returned pointer before testing its predicate. Compaction never folds onto a base today (D73); folding across timestamps (#34) must load a separated base through `JobContext::blob_files` first.

### Proposed decision: a FIFO `Drop` accounts for the blob pointers of the SSTs it drops
A `Drop` (#32) removes whole SSTs without a job, so it reports no `blob_live_delta`, and the blob files its SSTs point into would never reach zero (a space leak, caught by the `check_blob_accounting` invariant).

**Interim behavior:** before submitting a `Drop`, the engine reads the dropped SSTs within the tablet's rows when the family has blob files. It counts `16 + len` for every blob pointer they hold, as a job does for the puts it drops, and commits the deltas with the `RemoveSst`s through `blob_edits`, so a file that only those SSTs referenced is dropped in the same commit. The read happens in one slice of the compaction task. Recording referenced blob bytes per SST (#240) would make it a manifest lookup.

### Proposed decision (refines D29): separated values are pinned, not copied
D29 lists "blob reads" among the values `CellData` copies.

**Interim behavior:** a separated value read from its blob file is returned like an SST value: pinned in the block cache (`BlobReader` caches records up to `min(1 MiB, capacity / 8)` at low priority, D69, and hands larger ones out pinned but uncached), and copied only when it is at most `CellData::INLINE_MAX` (128) bytes. Separated values are at least `blob_threshold` bytes, so a copy would cost a large memcpy on every read.

### Proposed decision: `drop_table` drops the table's blob files
FORMAT §9.3's `DropTable` drops the table's tablets and SSTs; blob files belong to a family, and nothing dropped them.

**Interim behavior:** `drop_table` commits a `DropBlobFile` for every blob file of the table's families in the same manifest commit as its `DropTable`, so their extents are retired at that version. FORMAT §7 says so. No implicit rule was added to `DropTable` itself.

### Proposed decision: what holds a blob file's extents after it is dropped
**Interim behavior:** as for SSTs. A dropped file's extents are retired at the commit's version and reclaimed once no view (in-process or reader process, D61) can reach them; a view keeps the file's `BlobReader` (opened lazily, shared across versions by blob id, `OpenBlob`), so a snapshot taken before a blob GC keeps reading the old file. Its cached records are erased at the drop; an old snapshot that reads the retired file again re-caches records under the dead id until the cache evicts them (harmless: blob ids are never reused, so nothing else reads them).

### Q: shrink and blob extents
D160's `shrink` relocates SST extents only. Blob extents past the shrink point are treated like in-flight output and skipped, so they set a floor on how far the file shrinks.

**Interim behavior:** skipped. Relocating them is #231.

### Q: values larger than D16's write-time limit
Blob files can hold values up to `2^32 - 1` bytes, but every value still passes through one WAL record and one memtable entry before a flush separates it.

**Interim behavior:** D16's limit stays (`ValueTooLarge` above `min(WAL segment payload, 64 MiB, half the arena)`). Lifting it is #230.

**Coordinator:** confirmed. The two open questions are tracked: shrink relocating blob extents (#231) and values above D16's write-time limit (#230). Blob GC picking by referenced files is #240.

<a id="d181"></a>
## D181 — File format version 2 (approved; engine, format, #235)
### Proposed decision: format version 2
A 0.1.0 build reads a `Blob`-tagged value as empty bytes (`decode_value` falls back), so it would return wrong values from a file with blob files instead of refusing it.

**Interim behavior:** `FormatVersion::CURRENT` is 2 (`MIN_READABLE` stays 1): every structure is written with version 2, so 0.1.0 refuses the file at the superblock with `UnsupportedFormat`, and this build reads 0.1.0 files (upgraded to version 2 at the first commit). FORMAT §12 and the changelog say so; the golden files were regenerated. A per-file feature flag (version 2 only once a blob file exists) would keep untouched files openable by 0.1.0, but the pager writes the superblock version without knowing the catalog, and pre-1.0 the format may change in any release.

**Coordinator:** confirmed. A 0.1.0 binary refuses a version-2 file instead of reading separated values as empty.

<a id="d182"></a>
## D182 — Backup copies the separated values the snapshot references (approved; engine, #58, #238; amends D120, refines D177)
### Proposed decision (amends D120): `backup` copies the values the snapshot references
D120 refused `backup` of a database with blob files (`Unsupported`) until #58.

**Interim behavior:** with the two-phase backup (#262, #268), phase 1 copies the memtables into temporary SSTs with every value inline: blob files written there would belong to the copy, while phase 2 reads pointers through the source's blob files, and the temporary extents are freed after the merge. Phase 2 reads each source pointer through the snapshot's SST view (its pinned manifest version keeps the source blob files' extents) and writes the values, with the large values of the temporary SSTs, through the same separating sink a flush uses. The copy gets its own blob files (fresh ids from 1) holding exactly the values its SSTs reference, all live. Blob files are not copied extent by extent: that would also copy garbage and old files' layout. Each slot's merge (source and temporary SSTs) is clamped to its tablet's rows: after a split, children share SSTs that hold their siblings' rows too, and before this each child's copy repeated them.

**Coordinator:** confirmed. With the two-phase backup (D177), phase 1 writes values inline and only phase 2 separates.

<a id="d183"></a>
## D183 — Level outputs are cut into power-of-two pieces so a file at rest stays near its live size (approved; compaction, pager, sst, #185, #273, #276; refines D160)
### Built (#185): level outputs are cut into power-of-two pieces, at most half the stream each
The coordinator approved option 1' below (no format change), and it is built as follows.

- **Where.** In the compaction job's sink (`pigeonhole_compaction::job`), for outputs at levels ≥ 1, through `output_piece_bytes(remaining, target)`.
  - While more than the target's class remains, a piece is that class (64 MiB SSTs by default).
  - Otherwise a piece is the power of two at or below half the remaining stream, and at least 1 MiB. Once less than 2 MiB remains, the minimum drops to 64 KiB.
  - L0 (flush, FIFO's L0 merges) still writes one SST per flush.
- **Remaining.** The input bytes not yet read, scaled by the ratio of output written to input consumed so far, once at least 1/64 of the input is read. The open SST's data bytes come from the new `SstWriter::data_len`.
- **Cut.** Each output's extent is exactly its piece. The output is cut between rows (D78) once less than 1/32 of the extent is left: the index, filters and footer fit in that. If a later projection calls for a smaller piece, the output is cut once it holds that piece. At finish the extent is trimmed to the output's class.
- **`Engine::compact`.** A full compaction re-cuts a lone SST larger than one piece, where it used to move it trivially (a flush output would otherwise stay whole). An SST holding a single row cannot be cut and is left alone.
- **Blob separation (#33).** A separated value leaves only its pointer in the SST, and the sink sees the pointer. So pieces are sized from SST bytes (input and output lengths), never from blob bytes. Flushes separate too, so a compaction's inputs and outputs compare like for like. Blob files keep their own extents (64 KiB to 1 MiB, several per file).
  - Measured: 20 MiB of separated 1 KiB values compacts into 5 SSTs holding 355 KB.
  - The file at rest is still 1.60× its data, because `shrink` does not relocate blob extents (#281).
- **Not done here.**
  - Backup copies (`copy_at`) still write through the flush sink, so a backup's last SST can be half empty.
  - The #200 `shrink` notes (reserve holes for the largest extents; release a skipped extent's claim at once).

### Built: measured (deterministic test `crates/engine/tests/footprint.rs`, file sizes only)
The setup is the probe's (1 shard, 1 KiB incompressible values, delete to the kept fraction, then `compact` and `shrink`), on `SimVfs` and an application-owned shard. The test asserts file ≤ 1.2 × live + 384 KiB (1.3× under 1 MiB of data), where 384 KiB is the header unit, a manifest snapshot unit and the manifest log's 256 KiB extent.

| Load, kept | Live | File | Ratio | SSTs (was) |
|---|---|---|---|---|
| 5 MiB, 100% | 5.2 MiB | 6 MiB | 1.16× | 11 (1) |
| 20 MiB, 100% | 20.8 MiB | 22.25 MiB | 1.07× | 14 (1) |
| 50 MiB, 100% | 51.9 MiB | 54.25 MiB | 1.05× | 15 (1) |
| 50 MiB, 10% | 5.2 MiB | 6 MiB | 1.15× | 11 (1) |
| 50 MiB, 1% | 0.5 MiB | 1 MiB | 1.91× | 5 (1) |

- **Small files.** At 0.5 MiB of data the fixed metadata dominates: the SST extents total 576 KiB (1.05× the data), and the manifest log's 256 KiB extent sets the rest.
- **SST counts.** A run's count grows with the log of its tail, about 2·log2(tail / 64 KiB). That is more than the model's 5–10 because the 64 KiB minimum applies to every stream's last 2 MiB.

### Proposal (approved as option 1'; kept for the measurements): cut level outputs into power-of-two pieces, at most half the stream each (#185)
A file at rest is 1.2–3.8× its live data. Extents are power-of-two sized and aligned to their size, unit 0 (the header) is never free, and an SST takes the class above its length. So a file is at least twice its largest extent, and the last SST of a compaction is often half empty.

### Measured (file sizes only)
The probe ran on a CI runner (Sweep workflow on the scratch branch `scratch/footprint-probe`, test `footprint_probe`, `PIGEONHOLE_FOOTPRINT=1`). Setup: 1 shard, 1 KiB incompressible values, `max_versions(1)`, delete to the kept fraction, `compact` twice, `shrink`. A Python model of the allocator (aligned buddy, lowest fit, unit 0 reserved, largest first as `shrink` places them) reproduces every measured size exactly. The option columns come from that model fed with the measured SST lengths.

| Load, kept | Live (SSTs) | Now | Opt 1: power-of-two cut | **Opt 1': power-of-two, ≤ half the stream** | Opt 4: unaligned exact extents | SSTs now → 1' |
|---|---|---|---|---|---|---|
| 5 MiB, 100% | 5.2 MiB | 16 MiB (3.08×) | 8 MiB (1.54×) | **6 MiB (1.16×)** | 5.2 MiB (1.01×) | 1 → 5 |
| 20 MiB, 100% | 20.7 MiB | 64 MiB (3.08×) | 32 MiB (1.54×) | **22 MiB (1.06×)** | 20.8 MiB (1.00×) | 1 → 7 |
| 50 MiB, 100% | 51.9 MiB | 128 MiB (2.47×) | 64 MiB (1.23×) | **53 MiB (1.02×)** | 51.9 MiB (1.00×) | 1 → 10 |
| 50 MiB, 10% | 5.2 MiB | 16 MiB (3.08×) | 8 MiB (1.54×) | **6 MiB (1.16×)** | 5.3 MiB (1.02×) | 1 → 5 |
| 50 MiB, 1% | 0.5 MiB | 2 MiB (3.84×) | 1 MiB (1.92×) | **0.6 MiB (1.2×)**, with a 64 KiB minimum for streams under 2 MiB | 0.6 MiB (1.20×) | 1 → 5 |
| 200 MiB, 50% | 103.7 MiB | 192 MiB (1.85×) | 128 MiB (1.23×) | **105 MiB (1.01×)** | 103.8 MiB (1.00×) | 2 → 12 |
| 500 MiB, 100% | 518.7 MiB | 640 MiB (1.23×) | 576 MiB (1.11×) | **520 MiB (1.00×)** | 518.8 MiB (1.00×) | 10 → 17 |

### Options
1. **Cut outputs at power-of-two sizes** (#185's suggestion). The stream's binary decomposition (51 MiB → 32 + 16 + 2 + 1). This halves the overhead, but the floor stays at twice the largest piece: an aligned class-`c` extent cannot start at unit 0.
1'. **The same, with each piece at most half the remaining stream** (recommended). Every piece then has room below it, and the pieces pack to within about a minimum piece of the live data. Pieces are full power-of-two classes up to `target_sst_bytes`, so large databases keep 64 MiB SSTs and only each stream's tail splits, into about 2·log2(tail / min piece) SSTs. The minimum piece is 1 MiB, or 64 KiB for streams under 2 MiB.
2. **A smaller `target_sst_bytes`.** This only bounds the hole, it is a throughput trade that needs bench numbers, and 1' makes it unnecessary.
3. **`shrink` splits an SST that has no hole of its class.** That is compaction-shaped work inside `shrink`, made unnecessary by 1'.
4. **Unaligned or exact-length extents, or multi-extent SSTs.** About 1.00×, but a **format change**. 0.1.0 readers reject a misaligned extent at load (`alloc::unit_of`), and multi-extent SSTs change `SstMeta`. It also needs a new allocator (best-fit over arbitrary ranges, with its own fragmentation), and it gains at most about 2% over 1' on these probes.

### Recommendation: 1', no format change
- **Where.** In `SstSink` (flush.rs), for outputs at levels ≥ 1 (compaction outputs, `Engine::compact`, backup copies). The output target becomes `min(class(target_sst_bytes), class(remaining / 2))`, rounded to a power-of-two class of at least the minimum piece, and an SST is cut once it fills its class. Cuts still fall between rows (D78), within the existing "less than an eighth left" slack.
- **Remaining.** It is estimated from the inputs: a compaction's input bytes not yet read, a flush's memtable bytes, a backup's sources. Garbage collection makes outputs smaller than inputs, so pieces can come out a class large and the last one trims to a smaller class. That costs a few percent, not 2×.
- **L0 stays one SST per flush.** Splitting flush outputs (or FIFO's L0 merges) would multiply the L0 file count and trigger compaction and the write stall sooner. Their rounding waste is transient, because L0 is compacted down.
- **FORMAT.md.** No change: extents stay aligned power-of-two classes. Only the writer's cut policy changes, and FORMAT.md §8.2 needs no edit.
- **Migration.** None is needed. 0.1.0 files open and stay valid. Each compaction rewrites its outputs in the new shape, and `compact()` then `shrink()` re-lays a whole file at once. Files written by the new code still open in 0.1.0 (same format), so downgrade works.
- **Costs.**
  - More SSTs per sorted run: on the probes 1 → 5–10 for small runs, and 10 → 17 at 500 MiB.
  - Each SST adds an index top level, filters, an open reader and a manifest entry of about 100 B.
  - Point reads still touch one SST per level, and a scan iterates the same number of runs.
  - The sparse-wide gate's data sizes put most bytes in full 64 MiB SSTs, so I expect no measurable read change; the coordinator's bench run would confirm.
- **Also folded in**, from the #200 review notes: `shrink` reserves holes for the largest extents first (or retries them after the small moves of a round), and releases a skipped extent's `busy_ssts` claim at once.

**Interim behavior:** superseded; see "Built" above.

**Coordinator:** confirmed (option 1'; no format change, no migration). Measured after compact + shrink: 1.05–1.16× live data for 5–50 MiB (was 2.47–3.08×). L0 stays one SST per flush; backup copies still use the flush sink.

<a id="d184"></a>
## D184 — Blob GC follows the blob references each SST records (approved; compaction, engine, format, #240, #285; refines D180)
### Proposed decision (refines D180): blob GC follows the SSTs' recorded blob references
D180's blob GC rewrote each candidate blob file out of every slot of its family, and remembered in memory which slots it had emptied, so after a reopen (or for a tablet a merge created or a move brought) a slot could be rewritten once more for nothing.

**Interim behavior:**
- Every commit that adds a new SST also commits `Edit::SstBlobRefs` (manifest tag 13, FORMAT §9.3). It lists the blob files the SST's puts point into, with the bytes they reference (`16 + len` per pointer), and an empty list when the SST holds no pointer. Flushes, compactions, open-time spills, backups and shrink copies all write one. Writers count with `note_blob_ref` as they add entries, so a reference costs no extra I/O. The compaction job returns its outputs' references from the added `CompactionJob::finish_with_blob_refs` (`finish` is unchanged). The catalog drops the references of SSTs no tablet references after each batch, so a trivial move or a split keeps them.
- Blob GC picks a slot for a candidate file only when one of the slot's SSTs records a reference into it. An SST with no record (written before tag 13) counts as pointing anywhere. After a slot's blob GC commits, its new SSTs record no reference into the file, so the slot is not picked again, across reopens too. The in-memory record of emptied slots is gone.
- A FIFO `Drop` (D180) uses a dropped SST's recorded bytes instead of reading it when the SST holds only this tablet's rows (not shared with a sibling and not inherited from a split's parent). Otherwise it still reads the SST within the tablet's rows, since the record counts every row.
- `check_blob_accounting` (test hook, run at every full dump of the engine model harness) also checks that each SST's record equals the pointers it holds.

**Coordinator:** confirmed. Independent review found no must-fix: an SST without a record counts as pointing anywhere, so GC stays conservative for pre-#240 files and terminates once outputs carry records. Tag 13 is skipped by length by builds that predate it.

<a id="d185"></a>
## D185 — `shrink` relocates blob extents (approved; engine, #231, #286; amends D160)
### Proposed decision (amends D160): shrink relocates blob extents
D160's `shrink` relocated SST extents only, and treated blob extents like output in flight, so a blob file at the tail set a floor on the file.

**Interim behavior:**
- `shrink` also maps the catalog's blob extents and relocates any past the shrink point into a free extent of the same class below. The copy keeps the extent header (file id and position), so it can replace the original in place.
- The move commits as a `PutBlobFile` whose extent list has the copy at the same position, computed against the catalog at commit time. If the file was dropped meanwhile (blob GC or `drop_table`) or no longer lists that extent, the copy is abandoned. A refused request frees the copies.
- Blob files are never written once published, so a move needs no claim. A blob GC or backup reading the file keeps reading the old extents through its view, which pins their manifest version.
- The manifest retires the extents a file's new list drops. Views build a new blob reader for a file whose extents changed, while older views keep theirs, which reads the retired extents they pin. Cached records stay valid: they are keyed by the file and the logical offset, and the bytes are the same.

**Coordinator:** confirmed. The copy is unsynced until the root commit syncs data before the superblock (D58), as for SST moves; a crash before the commit leaves the copy in unnamed, free space.

<a id="d186"></a>
## D186 — Counter families: stored kind, 0.1.0 semantics, seqno-scoped deletes, and compaction that never changes reads (approved; pigeonhole, engine, compaction, format, sim, #274, #289; refines D179)
### Proposed decision: the family kind is stored, not derived from the operator
A family's kind is a new `FamilyOptions::kind` (`FamilyKind::{Standard, Counter}`), appended as one byte to the family options in the manifest (`PutFamily`). Edit bodies already ignore trailing bytes, so a body that ends before the byte reads as `Standard`; an unknown value is `Corrupt`. It relies on format version 2 (D181): a 0.1.0 reader would otherwise ignore the byte and read counter families as Standard. FORMAT.md §9.2 says so. A golden file of a 0.1.0 snapshot (`manifest_snapshot_0_1.bin`) pins the old encoding.

Deriving the kind from `merge_operator == "pigeonhole.i64_add"` was rejected: every 0.1.0 family stores that name, so every existing family would have become a counter family and refused byte puts.

**Interim behavior:** as above. `Family::counter()` stores kind `Counter` with `pigeonhole.i64_add`; the engine refuses a counter kind with any other operator (`InvalidArgument`). The kind is fixed at creation (an existing family keeps its stored options).

### Proposed decision: 0.1.0 families keep 0.1.0 semantics
A family written by 0.1.0 has the `i64` operator and kind `Standard`. It keeps exactly its 0.1.0 behavior: `incr` writes at the commit timestamp, runs of operands fold across timestamps at read (D41), byte puts are allowed (and fail at read under an `incr`, `MergeFailed`).

The issue suggested treating such a family as a counter family for reads, with new `incr`s at the fixed timestamp. That breaks reads: per-timestamp resolution of old commit-timestamp operands returns one increment per version instead of the sum, and a 0.1.0 base `put_i64` at commit timestamp T would sit above every later increment at timestamp 0, so the counter would stop moving.

**Interim behavior:** as above; `incr_at` on such a family is refused (buckets are for counter families). `Family::default().merge_operator("pigeonhole.i64_add")` creates one. CHANGELOG has the migration note (copy counters with `get` + `put_i64` into a counter family).

### Proposed decision: in a counter family a delete hides only earlier writes (by seqno)
D179 says deletes "work normally". With the fixed timestamp they cannot keep timestamp scope: a `delete_column` at commit timestamp T covers timestamp 0 forever, so every later `incr` would stay hidden until a compaction purged the marker. Following Bigtable, a delete in a counter family hides the entries in its timestamp scope with a **lower seqno** only. Same-commit writes are not hidden (equal seqno; D34 still collapses same-cell writes). Snapshots read the operands they see, ordered by seqno.

**Interim behavior:** `ResolveOptions::counter` / `GcConfig::counter` / `ModelFamily::counter` implement it; ordinary families are unchanged.

### Proposed decision: counter-family compaction never changes reads
Consequences of seqno-scoped deletes in compaction (`gc.rs`, `decide_counter`):
- An entry is dropped when a delete in its own stripe with a higher seqno covers it; a delete is redundant next to a wider one in its stripe with a higher seqno; bottommost purges of stripe-0 deletes below `min_ts_above` are unchanged.
- Two operands are combined only if no other source of the slot can hold a delete with a seqno between theirs: a delete there would hide the older but not the newer. `GcPolicy::other_sources` lists the slot's non-input SSTs (with key ranges) and memtables / prepared shares (unbounded keys) that may hold a seqno at or below the newest input seqno; `None` means unknown (no combining). Sources entirely above the inputs are left out, so in practice a hot counter compacts to one entry.
- Versions beyond `max_versions` are **never purged**: a later delete of a newer bucket, or its expiry under a TTL, must show the older one again (found by the compaction proptest; seeds kept in `counter_versions_outlive_newer_expired_buckets`). Reads apply the limit; space is bounded by the TTL. Follow-up decision: #284.

So `Model::purge` is a no-op for counter families.

### Q: a counter family with a TTL
TTL applies per bucket (`ts + ttl <= now`), so the fixed timestamp 0 would expire at once. **Interim behavior:** in a counter family with a TTL, a put or operand without a timestamp fails with `InvalidArgument` (use `incr_at` / `put_i64_at`). Alternative: exempt the fixed timestamp from the TTL.

### Q: two increments of one counter in one mutation
D34 collapses same-cell writes in a commit to the last one, so `.incr(c, 1).incr(c, 2)` adds 2. This was already true in 0.1.0 (both at the commit timestamp). Counters could combine same-commit operands instead. **Interim behavior:** unchanged (D34), documented on `RowMutation::incr`.

### Q: `#[non_exhaustive]` on option structs
Adding `FamilyOptions::kind` is semver-breaking (exhaustive public struct), hence the 0.2.0 bump. `#[non_exhaustive]` on `FamilyOptions` (and `ModelFamily`) would make later fields additive, but also forbids `FamilyOptions { .., ..Default::default() }` outside the defining crate, which the engine, compaction and test crates use in dozens of places; they would need constructors or builder methods first. **Interim behavior:** not added in #274; proposal: add builder-style setters to `FamilyOptions`, migrate callers, then mark it `#[non_exhaustive]` (with `ModelFamily`) before 1.0. (`ResolveOptions` and `GcPolicy` are already non-exhaustive.)

### Note: model-check harnesses and arena slots
The engine and public harnesses keep four families per table; a new `counters` test target in each runs the same suites with `f`, a 0.1.0-style `counter`, and the counter families `sum` and `sum_ttl` (the family set is chosen by `env!("CARGO_CRATE_NAME")`). Six families overflowed a tablet-off shard's arena (16 slots), which stalls or hangs: #283.

**Coordinator:** confirmed, after independent review (#289). Open points:
- A counter family with a TTL: the interim rule (untimed counter writes refused) stands. It can be relaxed later without breaking callers.
- Two increments in one mutation: #295 proposes combining them, as Bigtable does (needs owner sign-off).
- `#[non_exhaustive]` on option structs: #296 (Phase 4).
- Purging buckets beyond `max_versions`: #284 (owner decision). Counter tombstones never purging at the bottom level: #290.
- Harness arena slots: #283.

**Owner:**
- #284: versions beyond `max_versions` in a counter family are never purged. Reads apply the limit, and a TTL bounds the space. This matches Bigtable, whose version limits are also lazy cleanup rather than a read guarantee. #284 is closed.
- #295: increments of one counter within one mutation combine (`incr(c, 1).incr(c, 2)` adds 3), as Bigtable's read-modify-write rules do. This is an exception to D34 for counter families.

<a id="d187"></a>
## D187 — Counter-family deletes purge at the bottom level by seqno (approved; compaction, engine, #290, #298; refines D70, D186)
### Proposed decision: counter-family deletes purge at the bottom by seqno (#290)
D70 purges a bottommost delete only below `GcPolicy::min_ts_above`. Every source that holds a fixed-timestamp counter (D179) has minimum timestamp 0, so in a family that uses `incr` the bound stays 0 and no counter tombstone ever purged.

A counter delete hides only entries with a lower seqno in its scope. So a bottommost delete visible at every read point (stripe 0) is also purged when no other source that may hold keys of its row (`GcPolicy::other_sources`, filtered by key range per row) starts at or below its seqno. Nothing outside the inputs is then old enough for it to hide, and what it hides in the inputs is dropped with it (same stripe, lower seqno). Sources the engine leaves out of `other_sources` start above the newest input seqno, so they never block a purge. Later writes take seqnos above `visible`, and prepared shares at or below it are listed. When `other_sources` is `None`, only the timestamp rule applies.

**Interim behavior:** implemented in `gc.rs` (`counter_purgeable`) for cell and column deletes and family markers. Reads never change, so `Model::purge` stays a no-op for counter families. Tests: `counter_deletes_purge_at_the_bottom_by_seqno`, and the proptest `counter_purges_preserve_reads_with_other_sources`, which splits rows between the inputs and another source with interleaved seqnos (whole columns and row markers, `(column, timestamp)` groups, or single entries, so a cell or column delete can land apart from what it hides). It catches a purge that ignores the other sources, for markers and for cell and column deletes, and operand combining that ignores them. The purge relies on compaction subranges being row-aligned (`CompactionTask::subranges`).

**Coordinator:** confirmed after independent review (#298). No path was found where the purge changes a read. The review's test gap (cell and column deletes landing apart from what they hide) is covered.

<a id="d188"></a>
## D188 — Values above the inline limit are separated into blob files at commit time (approved; engine, runtime, #230, #301; amends D16)
### Proposed decision (amends D16): puts above the inline limit are separated at commit time
D16 refused any value longer than `min(WAL segment payload, 64 MiB, half the shard's memtable arena)` with `ValueTooLarge`, because every value went through one WAL record and one memtable entry before a flush could separate it.

**Interim behavior:**
- That bound is now the *inline limit*. A put whose stored value is longer is written into a new blob file when its batch is routed (one `BlobSink` per family; blob extents of 64 KiB, larger once a value would span more than 256 of them). Larger fixed extents, trimmed to a short file, left free runs too short for the next one, so every separation grew the file. The files are committed in the manifest, whose root commit syncs their bytes, before the batch is submitted. They are pending (`Shared::large_pending`) from before that commit publishes them until the commit settles. The WAL record and the memtable entry hold the 17-byte pointer, and from there the value is handled like any separated value (flush, compaction, blob GC, #240 references, reads, backup).
- A stored value can be up to `2^32 − 1` bytes (the pointer's `len`), so a payload up to 4 GiB − 2. A merge operand above the inline limit is still `ValueTooLarge` (operands are never separated). The family's `blob_threshold` doesn't matter here.
- Same-commit collapse (D34) happens before separation: only the last mutation per (column, timestamp) of a batch is kept, so no value of a mutation the shard would drop goes to a blob file. Two default timestamps compare equal. An explicit timestamp equal to the commit's own cannot be detected at routing (the shard assigns it). So a value that a later mutation of the same column, with the other kind of timestamp, may displace goes to a blob file of its own. A shard that drops a pointer at apply reports its file (`Shared::large_dropped`), and the commit's guard releases it, so live bytes stay exact.
- Disk use: only a one-extent file is trimmed, and FORMAT §7 keeps one size class per file, so a multi-extent file keeps its last extent's unused tail. Letting a value span up to 256 extents (`BlobSink::spread_values`, additive) bounds that to one extent of at most about 2/256 of the value, or 64 KiB. With the sink's default four, a 65 MiB value used 96 MiB. Trimming the last extent of a multi-extent file would need a FORMAT §7 change (readers check the class), and isn't needed for this.
- Cost: one manifest commit per batch with such a value, on the submitting thread. `commit_from_thread` drives that commit itself, as `shrink` does, so this works on a shard-driving thread too (application-owned `commit_local` included).

### Proposed decision: releasing a refused commit's blob files
**Interim behavior:** a guard owns the new files from the manifest commit until the commit's outcome is known. It is attached to the reply with the runtime's additive `Notifier::on_resolve`, so it runs even if the caller drops the `PendingCommit`. What decides is where the commit stopped, not its error: `Busy` and `ValueTooLarge` also come back after the WAL append (a memtable apply that fails poisons the shard), and a cross-shard commit whose decided apply fails returns `Busy` after its COMMIT is durable.
- A shard notes a batch's files as logged when its WAL stream accepts the record (a single-shard batch, or a cross-shard PREPARE), before it applies anything. From there the batch may be applied or replayed.
- The files get a `DropBlobFile` (queued through the manifest, never blocking the shard) when the guard is dropped before reaching a shard (an error, or an unwind, between the manifest commit and the submission), or when the commit didn't succeed and no shard logged the batch. A `check_and_mutate` whose predicate is false is refused before its append.
- A logged commit that failed keeps its files, and they stay excluded from the accounting check until the next open. The open-time sweep below drops the ones nothing points into, including those of a cross-shard commit aborted after a PREPARE was logged.
- A file the D34 collapse dropped at apply is released whatever the outcome.

### Proposed decision: the open sweep, with no pending marker
The coordinator asked whether the pending marker (a new manifest tag) could be dropped.

**Interim behavior:** no marker, no format change. Flushes, compactions, blob GC, backups and shrink copies always commit a blob file in the same manifest commit as the SSTs that point into it, and those SSTs carry `SstBlobRefs` (#240). So after WAL replay, a blob file that no SST record, no SST of its family without a record, and no recovered memtable entry points into can only come from a commit-time separation whose commit never became durable, or was refused before its release committed. It is dropped at open (`DropBlobFile`, extents retired). Recovery that does not apply a record (an aborted cross-shard share, D83) leaves its file unreferenced, which is right.

### Q: tests and deferred I/O
The engine model harness drives the shards on the thread that commits, and with `PIGEONHOLE_DEFERRED_IO=1` that thread must also complete the in-flight I/O of a background manifest commit. A commit-time separation waiting on the manifest there would never return.

**Interim behavior:** `Store::open_cfg` sets a 120-byte inline limit (values go up to 160 bytes), so about a quarter of the puts take this path under faults, crashes and tablet changes. It doesn't with deferred I/O (#306: drive deferred completions from a helper thread instead). The recovery oracle compares put values above 120 bytes by length, since a WAL record's pointer may name a file blob GC has dropped since. `large_values.rs` covers the paths directly (including an apply that fails with `Busy` after the WAL append, through the `fail_next_apply` hook, and a D34 timestamp collision), and `huge_value.rs` runs the one real round trip above 64 MiB in its own test binary.

**Coordinator:** confirmed after independent review (#301). The review's must-fix (release decided by where the commit stopped, not by its error) and its should-fixes (the D34 timestamp collision, 256-extent spreading) are in the text above. Deferred-I/O coverage is #306 (Phase 3).

<a id="d189"></a>
## D189 — Arenas are sized for their slots with tablet changes off too (approved; engine, #283, #307; amends D136)
### Proposed decision: arenas are sized for their slots with tablet changes off too (#283)
D136 sized arenas for many slots only with tablet changes on: at least 256 chunks, so 64 `(tablet, family)` slots per shard, and smaller chunks when the tablets placed at open need more. Off, an arena was cut into `budget / 64` chunks (capped at 256 KiB), so a budget under 16 MiB served 16 slots. A shard holding more (4 tables of 6 families on one shard: 24) starved when a flush froze every slot. Writes stalled until `Busy`, or the public harness hung.

**Interim behavior:** both modes use `arena_chunk_size`. With tablet changes off, the slots counted are those `shard_for` places at open (`Catalog::max_slots_per_shard`); tables and families created later fit while a shard stays within 64 slots, as with tablet changes on. At the default 64 MiB budget nothing changes (256 KiB chunks either way). `EngineOptions::arena_chunk_bytes` (hidden) pins a layout for tests that starve an arena on purpose (`milestone_b`). A regression target, `engine/tests/slots.rs`, runs the model check with six families on one tablet-off shard. Seed 5 stalled before.

**Coordinator:** confirmed. Smaller chunks can't fail at apply: an entry larger than a chunk takes a contiguous run, and admission waits for one (`values_larger_than_a_small_chunk_are_admitted_and_applied`).

<a id="d190"></a>
## D190 — `shrink` clears a region when a large extent has no hole below it (approved; pager, engine, #314, #319; amends D160)
### Proposed decision (amends D160): `shrink` clears a region when a large extent has no hole
D160's `shrink` moved each extent past its target into an existing free extent of its class below it, and left it in place when there was none. Small extents scattered through the lower file (one per aligned hole, with free units between them) could then keep a large extent at the tail even though the space below added up. #185's footprint shape (50 MiB loaded, 10% kept) ended at 1.35× its data under flush GC (#287) for that reason.

**Interim behavior:**
- When a round's extent cannot move (no room, as opposed to claimed by a compaction or unnamed), the pager looks for a region of its class and alignment below it whose occupants are all live, smaller and movable: SSTs no compaction holds, blob extents, and the manifest's snapshot and log (moved by a snapshot rewrite). Every occupant must also fit in a free extent outside the region and below the large one, which is checked on a copy of the free lists only. Of the first 16 such regions among the lowest 512, the one with the fewest occupied units wins, the lowest on a tie (`Pager::clear_for`, additive).
- The region is then held. Its free blocks, and each occupant's old extent once it is reclaimed, become placeholders: pending, never published, and never handed to another allocation. A flush between the rounds therefore can't take the space. The occupants move out in the same round (`Pager::relocate_below`, additive: below the large extent, not necessarily below themselves), committed like any other move. In a later round, once nothing but placeholders is left inside, `Pager::relocate_into` (additive) takes the whole region for the large extent in one step.
- One region is held at a time. If it goes unused, no other region is cleared in that `shrink` call. That happens when the large extent leaves the plan, or when a pinned version (a snapshot or a reader process) keeps an occupant's old extent from being reclaimed. Whatever is still held is released (`Pager::release_region`, additive) before the final truncation.
- Snapshot rewrites allocate lowest-first (`Pager::allocate_lowest`, additive), so the manifest settles in the lowest hole that fits rather than the smallest free class. The fit check assumes that. A rewrite whose new snapshot needs a larger class than the old one can still land above the large extent.
- Crash safety: placeholders are pending and never published, so they are free space at the next open (D8). The occupants' copies are ordinary relocations, published by the round's root commit or abandoned. `shrink_crash_points` also sweeps every write point of a shrink that clears a region of SSTs, blob extents and the manifest, then moves the large SST in.

**Coordinator:** confirmed after independent review (#319). No corruption, loss, leak or double free was found on any path. The review's should-fixes are in the text above: the region is held through the move, clearing stops after an unused region, and the fit check copies only the free lists.
