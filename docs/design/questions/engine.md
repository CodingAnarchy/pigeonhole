# Engine questions (Phase 2)

## Q: May a flush purge versions beyond `max_versions` when no other source of the slot holds a delete? (#287)
Approved by the coordinator to build (2026-10-08). This records the rule and its correctness argument, for folding into a decision.

**Why.** A flush wrote its memtable verbatim, so every overwritten version stayed in L0 until a *bottommost* compaction removed it (D70: the `max_versions` purge runs only bottommost, below `min_ts_above`). On sparse-wide's Zipf-hot rows (about 3,500 cells, hundreds of overwrites on the hot qualifiers), reads and scans step over all of them, and that sets the gate's p99 (#287). `crates/bench/examples/hotrow.rs` (one such row, load 1.7–2.5):

| Hot-row state | Row read p50, main | Row read p50, with flush GC | Scan p50, main | Scan p50, with flush GC |
|---|--:|--:|--:|--:|
| As written (versions in memtable and L0) | 976–1006 µs | 694–699 µs | 961–987 µs | 672–673 µs |
| Flushed (all versions in L0) | 784–786 µs | 365–366 µs | 907 µs | 415–419 µs |
| Fully compacted (the floor) | 258 µs | | 289 µs | |

On the full sparse-wide gate (`--scale full`, back to back, load 3.0 → 1.5): Pigeonhole goes from 16.7K to 18.4K ops/s and from a 729 to a 659 µs p99. Row read p99 goes from 848 to 766 µs, and scan p99 from 954 to 831 µs. SQLite EAV measured 18.5–20.4K ops/s at a 276–285 µs p99 in the same runs, so the gate is still missed. The rest is memtable versions: during a measured phase, reads stepped over 440M memtable entries against 38M SST entries (92% from memtables, about 440 per operation). Flush GC cannot reach those; that is #311.

**What the flush does.**
- **The GC run.** It runs its memtable through compaction's GC (`pigeonhole_compaction::StreamGc`, the same `Gc` and `decide`/`decide_counter`) as a **non-bottommost** job. That job has the same read points as a compaction: every live snapshot of the process plus the oldest reader pin (`compact::gc_snapshots`, shared with `gc_policy`).
- **What any flush may drop.** It drops what a non-bottommost compaction of the same entries could already drop: expired entries, puts and operands hidden by a delete at every read point that sees them, puts shadowed at the same timestamp, and redundant deletes. It combines operands within a stripe. Counter families go through `decide_counter` with `other_sources` unknown, so no operand is combined.
- **Tombstones are never purged at flush** (not bottommost).
- **One new rule, under a guard.** If no source of the slot other than the flushed memtable can hold a delete, versions beyond `max_versions` are purged among the memtable's own versions of each column (`GcPolicy::no_outside_deletes`; `decide` counts versions when bottommost as before, or when the guard holds). Counter families never get it (D186): `decide_counter` ignores the flag.
- **Blob pointers.** A dropped blob pointer lowers its file's `live_bytes` in the flush commit, as a compaction's `blob_live_delta` does (`compact::blob_edits` against the committing catalog). Memtables hold raw values today: flush separates large values after GC, so until commit-time separation (#301) lands this is empty.

**The guard.** The shard computes it on its own thread when it queues the memtable for flushing (`shard::flush_guard`; the item then carries a shared state, `FlushItem::guard`). It holds when all of these are true:
- **Other memtables:** none of the slot's other memtables holds a delete, the active one included. The shard marks a `MemEntry` on inserting any delete kind; that is process-local state, not in the shared layout.
- **Prepared shares:** no prepared, undecided cross-shard share writes the family. A share that does not decode counts.
- **SSTs:** every SST of the slot has `SstMeta::deletes == 0`.
- **Replay:** the shard is not replaying its WAL at open. Replay applies commits in stream order, not seqno order, so it never purges versions. This holds at every queue site, the closing shard's in-place flush included.
- **Not voided before:** a memtable whose guarded flush a delete voided is flushed again without the guard.

**Correctness.**
1. **Counting within the input is conservative.** Visibility of a version depends on deletes covering its timestamp, and on newer versions. Suppose a version `v` of column `c` is beyond `max_versions` among the memtable's own versions at every read point that sees it. Then at each such point at least `max_versions` newer versions of `c` in the memtable are visible, unless a delete hides one of them.
   - A delete inside the memtable is accounted for by the GC's stripes and `hide`, exactly as at the bottommost level.
   - Puts outside the memtable can only add newer versions, which pushes `v` further out. They can never bring it back.
   - So only a delete outside the memtable can make `v` visible again, by hiding a newer version: a `CellDelete` at its timestamp, or a column or family delete covering it but not `v`. Under D9 a delete hides by timestamp whatever its seqno, so an older SST's delete counts.
2. **The guard excludes every existing outside delete.** That covers the other memtables, including late-applied cross-shard shares that can carry seqnos below the flushed memtable's, the prepared shares that will land, and the SSTs. Compactions only drop deletes or keep them, and a compaction output holds no delete its inputs lacked. So no SST that appears between the queueing and the flush adds a delete the guard didn't see. A tablet merge brings in other rows, which cannot hide these columns.
3. **Concurrency: a delete before the install voids the purge, and one after it waits.** The frozen memtable stays in the view until the flush installs. A delete committed between the queueing and the install would be visible through it: it would hide the newer version, show `v`, and then the install would drop `v` with no write in between. D74 covers only a delete after the install. (Found in review; the first version of this PR argued it away as a later write.) So the guard is a three-state handshake (`shard::GUARD_*`):
   - **Admission.** Every member the shard admits that writes a delete in a family with a guarded flush in flight (`admit_against_guards`, before room reservation and before its seqno becomes visible) moves that flush from *in flight* to *voided*. If the flush is already *installing*, a local commit waits for the outcome, as a stalled group does, and a cross-shard PREPARE is refused for the coordinator to retry. A commit record carries no new mutation: its share was admitted as a PREPARE.
   - **Install.** The flush's manifest commit moves each guarded item from *in flight* to *installing* inside its catalog closure, under the manifest writer, before the edits are fixed. If it finds *voided*, the request is refused: its outputs are abandoned, the shard flushes the memtable again without the guard at once (not counted as a failure), and the members that waited run again.
   - So every delete in the family is either admitted before the install decision, and voids it, or becomes visible only after the view without `v` is published, which is a later write in D74's sense. The same race in bottommost compaction (`min_ts_above` sampled at plan time, for explicit-timestamp writes) is the coordinator's separate issue.
4. **Snapshots and stripes are exactly compaction's.**
   - The read points are the live snapshots and the oldest reader pin. A reader process pins its seqno before it loads a view, so it can read an older seqno through the view this flush publishes (D61, D70).
   - Expiry uses the flush's clock.
   - An in-process snapshot taken before the flush keeps reading its own view, which still lists the memtable.
   - **A snapshot pins its seqno before it loads the view.** `Engine::snapshot` reads the visible seqno and registers it under the live-seqno lock that `gc_snapshots` reads under (`SeqnoPin::pin_visible`), then loads the view. A GC that read the list earlier saw only inputs at or below that seqno, and one that reads it later keeps its versions. Before this, the pin was registered after the view loaded, so a snapshot could read its seqno through a view whose flush GC never saw it. Every flush drops history now, which makes that likely. `Engine::get_latest` takes no pin: it reads the visible seqno, loads the view, and uses them only if the visible seqno has not moved (then no flush in between had inputs above it). Otherwise it reads again, and after four tries it takes a pinned snapshot. Seqno before view is kept everywhere (blob visibility relies on it).

**Tests.**
- **`crates/engine/tests/flush_gc.rs`** (application-owned shard, SimVfs):
  - a snapshot taken before the flush still reads every version it saw, through the view the flush publishes;
  - an older SST's cell delete keeps the version it exposes;
  - a delete admitted while a guarded flush is in flight voids its purge (the review's scenario);
  - a snapshot whose seqno is pinned before a flush publishes still reads its version through the new view (a hook between the pin and the view load).
  The first three also run with the GC broken on purpose (`Engine::mutate_flush_gc`: guard dropped, snapshot floor dropped, voids ignored), and the reads must then go wrong. The fourth fails with the old order (checked by hand).
- **The model** (`pigeonhole_sim::Model::purge_versions`) allows exactly this purge:
  - step 2 of `purge` only, with no `min_ts_above` condition, over the flushed memtable's exact input seqnos (a flush records them in its `CompactionRecord`);
  - only if the model agrees the guard held: no unexpired delete of the family in the slot's rows outside the inputs with a seqno at or below the newest input or the install point (the visible seqno when the commit took the purge, recorded as `CompactionRecord::install_seqno`);
  - `purge_versions_respects_the_flush_guard` checks both cases.
- **The harness** applies flush records like bottommost compactions. Every flush now records, because flush GC drops history like any compaction, which raises the floor for historical reads.
- A random-seed mutation test was tried and dropped. With the guard dropped, 20 seeds of a concentrated workload never produced the one exposing pattern: an older `CellDelete` at exactly the timestamp of one of the memtable's newest versions. An older column or family delete cannot expose anything (it hides `v` too); deletes before the install are voided (above), and those after it are the D74 case. The deterministic tests above cover it instead.

**Interim behavior:** as described, behind no option. A flush with no prepared share, no delete in the slot's other memtables or SSTs, and a family with `max_versions` purges overwritten versions as above. Otherwise it applies only the non-bottommost compaction rules.
