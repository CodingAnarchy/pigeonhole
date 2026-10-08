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

**The guard.** The shard computes it on its own thread when it queues the memtable for flushing (`shard::flush_guard`, stored in `FlushItem::no_outside_deletes`). It holds when all three are true:
- **Other memtables:** none of the slot's other memtables holds a delete, the active one included. The shard marks a `MemEntry` on inserting any delete kind; that is process-local state, not in the shared layout.
- **Prepared shares:** no prepared, undecided cross-shard share writes the family. A share that does not decode counts.
- **SSTs:** every SST of the slot has `SstMeta::deletes == 0`.
- **Replay:** the shard is not replaying its WAL at open. Replay applies commits in stream order, not seqno order, so it never purges versions.

**Correctness.**
1. **Counting within the input is conservative.** Visibility of a version depends on deletes covering its timestamp, and on newer versions. Suppose a version `v` of column `c` is beyond `max_versions` among the memtable's own versions at every read point that sees it. Then at each such point at least `max_versions` newer versions of `c` in the memtable are visible, unless a delete hides one of them.
   - A delete inside the memtable is accounted for by the GC's stripes and `hide`, exactly as at the bottommost level.
   - Puts outside the memtable can only add newer versions, which pushes `v` further out. They can never bring it back.
   - So only a delete outside the memtable can make `v` visible again, by hiding a newer version: a `CellDelete` at its timestamp, or a column or family delete covering it but not `v`. Under D9 a delete hides by timestamp whatever its seqno, so an older SST's delete counts.
2. **The guard excludes every existing outside delete.** That covers the other memtables, including late-applied cross-shard shares that can carry seqnos below the flushed memtable's, the prepared shares that will land, and the SSTs. Compactions only drop deletes or keep them, and a compaction output holds no delete its inputs lacked. So no SST that appears between the queueing and the flush adds a delete the guard didn't see. A tablet merge brings in other rows, which cannot hide these columns.
3. **Concurrency: a delete committed after the guard is a later write.** A memtable freezes only once every seqno it holds is visible (`Shard::freeze`). Every commit that applies here afterwards has a seqno above the visible watermark, so above every entry of the flushed memtable. The exception is a prepared share, which the guard has already ruled out. A delete committed later that would have exposed `v` is therefore a later write in D74's sense: purges follow HBase semantics, and a later delete does not re-expose a version the store already purged. Bottommost compaction accepts the same race, since it samples `min_ts_above` when it plans. So there is no install-time re-check.
4. **Snapshots and stripes are exactly compaction's.**
   - The read points are the live snapshots and the oldest reader pin. A reader process pins its seqno before it loads a view, so it can read an older seqno through the view this flush publishes (D61, D70).
   - Expiry uses the flush's clock.
   - An in-process snapshot taken before the flush keeps reading its own view, which still lists the memtable.

**Tests.**
- **`crates/engine/tests/flush_gc.rs`** (application-owned shard, SimVfs). A snapshot taken before the flush still reads every version it saw, through the view the flush publishes. An older SST's cell delete keeps the version it exposes. Each test also runs with the GC broken on purpose (`Engine::mutate_flush_gc`: guard dropped, snapshot floor dropped), and the reads must then go wrong.
- **The model** (`pigeonhole_sim::Model::purge_versions`) allows exactly this purge:
  - step 2 of `purge` only, with no `min_ts_above` condition, over the flushed memtable's exact input seqnos (a flush records them in its `CompactionRecord`);
  - only if the model agrees the guard held: no unexpired delete of the family in the slot's rows outside the inputs with a seqno at or below the newest input;
  - `purge_versions_respects_the_flush_guard` checks both cases.
- **The harness** applies flush records like bottommost compactions. Every flush now records, because flush GC drops history like any compaction, which raises the floor for historical reads.
- A random-seed mutation test was tried and dropped. With the guard dropped, 20 seeds of a concentrated workload never produced the one exposing pattern: an older `CellDelete` at exactly the timestamp of one of the memtable's newest versions. Column and family deletes cannot expose anything, and later deletes are the accepted D74 case. The deterministic tests above cover it instead.

**Interim behavior:** as described, behind no option. A flush with no prepared share, no delete in the slot's other memtables or SSTs, and a family with `max_versions` purges overwritten versions as above. Otherwise it applies only the non-bottommost compaction rules.
