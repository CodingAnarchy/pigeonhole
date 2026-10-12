# Decisions made in Phase 3 (D194–)

Indexed in [README.md](README.md). Numbers are permanent and continue from Phase 2; code and docs cite them as `Dn`.

<a id="d194"></a>
## D194 — Reads skip superseded memtable versions through a writer-only, process-local stale-tail index (approved; owner decision, 2026-10-09; memtable, engine, compaction, #387; alternative deferred as #397)
**Owner decisions.**
1. A writer-only acceleration structure is acceptable. Reader processes have no index: they read the same data with the same results, at today's cost.
2. The alternative, a skip offset stored in the shared-memory node, is not taken now. It would serve reader processes too, but needs FORMAT and `ShmLayoutVersion` changes, and since nodes are written once it could only point backward. It is kept open as #397 (Phase 4), to revisit before 1.0 only if reader-process latency matters.

**Why.** Under D193 the remaining Phase 2 gap is the wide-row read tail. On sparse-wide's hot rows a row read or scan steps, per returned cell, about 2.05 memtable entries and 0.017 SST entries: about 1.05 superseded memtable versions per cell. Each one passed costs about 460 instructions (Linux callgrind), whichever source it comes from: the merging heap re-sift, the resolver's column check and the source's own advance (about 63 of the 460 for a memtable). The resolver already stops resolving a column once its version limit is reached (`col_skip`), but it still steps every remaining version, up to `SKIP_STEPS` (8) before it seeks. Making the step cheaper was tried and measured: a tight skip loop (#375, +3.4% flushed), a runner-key cache (#377, +1.1–1.2%), other `SKIP_STEPS` values (all worse than 8), and an early flush (#383/#386, which moved the versions into L0 and made the gate p99 worse). What is left is not stepping the stale versions at all. A fully compacted hot row reads at about SQLite parity (about 1,340 instructions per cell against about 1,300), so the stale memtable versions are most of the gap.

**The idea.** When the writer inserts a node `e` whose level-0 successor at insert time is in the same column, it records `tail[e]`: that successor's own `tail` entry if it has one, else the successor. `tail[e]` is therefore a node of the same column after `e`, as far down the column's version chain as was known when `e` was linked. A reader that has finished a column (the resolver has set `col_skip`) and whose merging cursor's top source is that memtable, positioned on a node `e` with a `tail[e]` entry, moves that one source to `tail[e]` and then steps it, inside the memtable cursor and without the heap or resolver, while the key is still in the column. Then it re-sifts the heap once. A missing entry, any other source on top, or a cursor from a reader process means stepping exactly as today.

In the common overwrite pattern (each new version has a newer timestamp, so it is linked in front of the previous newest), the chain `p3 → p2 → p1` gets `tail[p3] = tail[p2] = p1`. A read that returns `p3` and finds `p2` next jumps to `p1` and steps once, to the next column. A hot cell with hundreds of versions is passed with one lookup and one step, instead of eight steps and a seek.

**Where it lives (FORMAT unchanged).** The index is process memory owned by the writer's `Memtable`, never in the arena or the shared layout. The skiplist nodes, the header, `ShmLayoutVersion` and FORMAT §11 stay exactly as they are, so nodes are still never modified after linking. `Memtable::reader()` hands its `MemtableReader` an `Arc` of the index, and `MemtableReader::open` (reader processes, from a published view) gets none. The index is a fixed-capacity, write-once, open-addressed table of `(node offset, tail offset)` pairs in `u32` atomics, sized from the memtable's chunk budget. The writer fills an entry before it links `e`, so a reader that reaches `e` finds the entry or, if the table was full, falls back to stepping. Entries are never changed or removed, and the table is dropped with the memtable. That lifetime is covered by the `Pin` the reader and its `ArenaSlice`s already hold, because `tail[e]` is always an offset in the same memtable.

**Correctness.** The claim is that every node passed over by a jump is one the read would have stepped past without effect.
1. **Same column, older.** Nodes are only inserted, never removed, and key order is row, family, qualifier, then timestamp descending, then seqno descending. Every node between `e` and `tail[e]` in key order, including nodes linked after the entry was written, is in `e`'s column and sorts after `e`.
2. **Only after the column is finished.** The jump is taken only where the resolver already discards the rest of the column (`col_skip`: the version limit, D76's folded `versions`, is reached, or a predicate failed). There, every remaining entry of the column, from every source, is discarded today. The jump changes how this source passes them, not which entries count.
3. **Deletes (D38, D9).** A `CellDelete` at timestamp `T` hides every version at exactly `T`, whatever its seqno, so it can "un-shadow" nothing older but must be seen before a version at `T` is returned. The resolver already processes every entry at the returned version's timestamp, across sources, before it returns that version and sets `col_skip`, so a delete at the same timestamp is never jumped. Deletes at older timestamps (cell, column or family) only hide versions the read has already decided not to return. Family delete markers sort before the family's columns, so a jump inside a column never passes one.
4. **Snapshots (D191) and time bounds.** The jump starts only after the resolver has applied the read's snapshot and timestamp bound to the column. Entries the snapshot cannot see, or above the time bound, are already skipped by those rules; entries below are discarded by (2). So the index needs no seqno and gives the same result at every snapshot.
5. **Merge operands and counters (D186).** A column whose returned value is still accumulating operands has not set `col_skip`, so its operands are stepped as today.
6. **Concurrent writer.** A reader that loads `next[0]` from `tail[e]` uses the same Acquire load as every step, so new links behind the tail are seen or not exactly as today.
7. **Compaction, flush and backup.** They do not use the index: their cursors have no version limit, and flush GC (D191) reads its memtable through the resolver-free `StreamGc`.

**Reader processes.** They have no index and keep stepping, with unchanged results and cost.

**Expected effect and costs.**
- **Reads:** about 1.05 stale steps per cell (about 480 instructions) become a lookup (a few tens of instructions) and one in-memtable step on the hot rows. That is roughly 10–15% of the as-written hot-row read and scan cost (about 2,560 instructions per cell), and more on the hottest cells, which set p99.
- **Writes:** each insert whose successor is in the same column costs one table insert. `commit-one` and `commit-sixteen` must stay within their floors (D193).
- **Memory:** two `u32`s per indexed node, plus slack, in process memory. That is roughly 10% of the arena bytes on the gate's node sizes. The capacity is fixed per memtable, so it never grows.

**Enabling it.** It is built off by default. It is turned on only if all of these hold, and otherwise closed (like #375 and #377):
- On Linux callgrind: as-written and flushed hot-row read and scan improve.
- No other shape regresses beyond noise (0.2%; `row` 1%), the write shapes included, and every absolute ceiling holds. macOS does not regress.
- Equivalence: the sim model and the engine's randomized read tests agree with the index on and off, with deletes at the same and older timestamps, snapshots, time-bounded and multi-version reads, merge and counter families, and a concurrent writer (loom on the table's insert and lookup).
- A gate run in a quiet window the coordinator schedules shows the wide-row read and scan p99 drop. If the instructions drop but the gate tail does not move, it is closed, like the early flush.

<a id="d195"></a>
## D195 — Three instruction ceilings are raised to main's counts, and a change to the ceilings is measured on the current main (owner decision, 2026-10-09; bench, CI; amends D193)
**Decision.** The D193 ceilings for `flushed` (1905 → 1912) and `compacted` (1282 → 1288) are raised to main 99f8929's counts times their 0.3% headroom. `get-sst` is raised from 17883 to 17983, with its headroom raised from 0.3% to 0.5%.

**Why.** The ceilings were measured on a main without #384 (scans reuse their resolvers), which merged just before them. #384 raised `flushed` by 0.4% and `compacted` by 0.5%, within the per-change thresholds, so main was above both ceilings as soon as they took effect. `get-sst` drifted 0.36% on main after #392 and #396, neither of which touches the SST read path: the code-placement variation already seen on other shapes. None of these is a regression to undo. With main above its ceilings, every PR failed the check.

**Process.** A PR that changes `crates/bench/baselines/instruction-ceilings.txt` merges only when its base is the current main, so its CI measured the code it sets the ceilings for. A PR whose base predates a change to that file is rebased before it merges. The coordinator's merge script enforces both. Any other raise still needs an owner decision (D193).

<a id="d225"></a>
## D225 — A participant's PREPARE may be checkpointed once its commit is settled, not once the coordinator checkpointed the COMMIT (coordinator decision, 2026-10-12; engine, #585; amends D116)

**Decided:**
- **"Settled"** means the coordinator has counted every participant's `ShareFlushed`, its own share included: every share of the commit is in SSTs, D24's condition for the COMMIT record. At that point it tells the other participants (`CommitSettled`, formerly `CommitCheckpointed`), and each may checkpoint past its PREPARE.
- **Before,** D116 kept a participant's PREPARE until the coordinator's checkpoint passed the COMMIT. The checkpoint-time notice and the `passed_commits` queue are gone.
- **A coordinator counts its own share directly,** not as a message sent to itself.

**Why.**
- **A checkpoint is a stream prefix,** so D116's rule made one stream's checkpoint wait on another's. Two cross-shard commits in flight together whose coordinators take part in each other log each other's PREPARE before their own COMMIT. For example, shard 0 logs PREPARE 715 (coordinated by 2) before COMMIT 719, and shard 2 logs PREPARE 719 (coordinated by 0) before COMMIT 715. Neither checkpoint can then pass. Close waits for empty logs and never returned, and live, the WAL stopped being trimmed past those records (#585, found by the #527 gate-step work, 0.2.1 included).
- **The self-sent share report** re-entered the close once per share, and a close with thousands of them overflowed the shard's stack.

**Why it is safe.** A replay that finds a COMMIT whose participant trimmed its PREPARE treats the commit as incomplete (D116's all-or-nothing replay) and applies none of it. Once the commit is settled, every share is already in SSTs, so nothing is lost and nothing is applied twice.
- `model_check` has a case for it (`settled_commits_trimmed_by_participants_survive_crashes`: 16 cross-shard commits in flight, frequent flushes, crashes between and inside commits, at 2–4 shards).
- The same case runs a second loop with tablets splitting, merging and moving (`tablet_ops_ppm` 20 000, the fast balancer), where a moved slot or a retired tablet reads as flushed through the published catalog.
- Telling participants before the shares are settled fails that case with `LostAckedCommit`. A tablet change that commits before the moving tablet's frozen memtables are flushed (the `frozen.is_empty()` wait in `progress_op` dropped) fails its tablet loop.
- `engine/tests/close_crossed.rs` closes after concurrent cross-shard commits on real files, under a watchdog.
