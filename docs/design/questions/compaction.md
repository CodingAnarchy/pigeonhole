# pigeonhole-compaction questions

Coordinator review of PR #36 confirmed the interim behavior of: `min_ts_above`, the `JobContext` fields, the other additive API, the `I64Add` tag rule, `versions = min(requested, max_versions)`, value predicate semantics, engine task handling, blob accounting and small-value copying. They are kept below for numbering. Still open: purges vs later explicit-timestamp writes (owner decision).

## Proposed decision (confirmed): `GcPolicy::min_ts_above` bounds bottommost purges
A bottommost compaction may drop a delete that is visible at every live snapshot, and versions beyond `max_versions`. Both are safe for the inputs, but data *above* the inputs (L0 files and levels not in the task, memtables) is newer by seqno and can still carry older user timestamps (written later with an explicit timestamp): a dropped column, family or cell delete would uncover such an entry, and an upper `CellDelete` at a kept version's timestamp would make a purged older version the newest. Either changes a read at a live snapshot, which done-when (2) forbids, and the random test finds it quickly.

**Interim behavior:** an added public field `GcPolicy::min_ts_above: Timestamp` (additive; `GcPolicy` is `#[non_exhaustive]`): the smallest timestamp of any entry above the inputs in the task's range, `u64::MAX` when there is none. Bottommost delete purges apply only to deletes with `ts < min_ts_above`, and the `max_versions` purge only to columns whose newest timestamp is below it. `GcPolicy::new` sets it to 0, which purges nothing at the bottom (always safe). The engine computes it from the `ts_range` of upper SSTs overlapping the range and the minimum timestamp of each memtable (which the engine has to track on insert; `Memtable` exposes only `seqno_range`). With default timestamps (D11) everything above is newer than old tombstones, so purging works normally. Expired data, and entries hidden or shadowed within the inputs, are dropped at any level regardless.

## Proposed decision (confirmed): additive `JobContext` fields `target_sst_bytes` and `clock`
`CompactionJob::run(deadline_nanos)` must compare against the Vfs monotonic clock, but `JobContext` has no `Vfs`; and nothing tells the job the output SST size (`PickerOptions::target_sst_bytes` lives in the picker).

**Interim behavior:** `JobContext::target_sst_bytes: u64` (default 64 MiB) and `JobContext::clock: Option<VfsRef>` (default `None`), both additive under D33. With a clock, `run` checks it every 64 units of work (a unit is a family marker or one `(column, timestamp)` group); without one, `run` does 4096 units per call, or everything when the deadline is `u64::MAX`. `SstWriterOptions::created_micros` is set from `GcPolicy::now`.

## Proposed decision (confirmed): other additive public API
None of these change a frozen signature:
- `ResolveOptions::time_range` and `ResolveOptions::route_time_range` (see the D22 amendment below).
- `MergingCursor::current()` and `sources()`, `FilteredCursor::inner()`: the engine pins a zero-copy value through the source the merged cursor is on (`ResolvedCell::from_source`).
- `CellResolver::set_upper_bound(Option<&[u8]>)` (a scan over deleted rows stops at its range end instead of running to the next visible cell) and `into_cursor()`.
- `VecCursor`: an in-memory sorted cursor, the mock source for tests and examples above.
- `ValuePredicate::matches`, `Levels::level_bytes`, `PickerOptions::level_target`, `KeyRange::{all, intersect}`, `CompactionPicker::options`, `CompactionJob::{entries_read, entries_written}`.
- `MergeRegistry`'s `Default` is now a manual impl equal to `new()`, so built-ins are always present as documented (the frozen stub derived it, which would have produced an empty registry).

## Q (deferred to Phase 2, #34): should compaction fold counter operands across timestamps?
Every `incr` gets its own commit timestamp (D11), so a hot counter accumulates one operand per increment and every read folds them all. Folding a run across timestamps (or onto its base) in compaction is not read-preserving in general: a later `delete_cell` at one operand's timestamp, or a `put_at`/`delete_column` with an explicit timestamp inside the run, splits it in the model; TTL expires operands one by one; and a `time_range` scan sees operands but not a base outside its range (D22).

**Interim behavior:** compaction combines operands only within one `(column, timestamp)` group and one snapshot stripe (preserving every read), never across timestamps and never onto a base. A bad base is therefore never folded (#21): the read keeps failing with `MergeFailed`. Proposal for the owner: fold a run (and its base) at the bottommost level when the family has no TTL and the run lies below `min_ts_above`, accepting that a later explicit-timestamp delete inside the run no longer splits it. **Deferred to Phase 2 (#34)** per review. Until then operands accumulate: the guide (data-modeling.md, counters) says so, and `cargo bench -p pigeonhole-compaction` measures it (`counter_get/operands_N`: about 0.3 µs for 1 operand, 3.4 µs for 100, 307 µs for 10,000, i.e. ~30 ns per operand).

## Q (open, owner decision pending): purges diverge from the model for later explicit-timestamp writes
D38 accepts that once compaction drops a cell delete, a *later* put at that timestamp is visible (the model never drops markers). The same holds for column and family deletes (a later put with an older explicit timestamp) and for `max_versions` (a later `delete_cell` of the newest version does not bring back a purged older one, as in HBase). `min_ts_above` makes every compaction read-preserving for the data that exists when it runs; the divergence is only for writes made afterwards.

**Interim behavior:** as described. The engine's model-checked suite must either not combine explicit-timestamp deletes/puts below purged tombstones with compaction, or teach the model the same purge rule.

## Q (confirmed): `I64Add` accepts only `ValueTag::I64` values
The model treats any 8-byte value as an `i64` base (it has no tags). Stored values have tags: `put_i64` and `incr` write tag `0x01`.

**Interim behavior:** operands and bases must be `ValueTag::I64` (tag plus 8 bytes); a `Bytes` value of 8 bytes is a `MergeError`. The engine's model adapter should map the model's 8-byte values to `put_i64` (the compaction tests do), and the guide already says to write counters only with `incr`/`put_i64`.

## Q (confirmed): `ResolveOptions::versions` and the family's `max_versions`
`ResolveOptions` has no `max_versions`, but the model caps every read at it, and reads must not depend on whether compaction has purged yet.

**Interim behavior:** the caller passes `versions = min(requested, max_versions)` (0 meaning unlimited on either side); documented on the field.

## Q (confirmed): value predicates on typed and blob values
D22 says a value predicate tests the newest visible value of a column; the byte-level meaning is unstated.

**Interim behavior:** the column is returned (all requested versions) iff its newest visible version matches. Byte predicates compare the payload (stored value without the tag); `I64` matches `i64` and varint values; a blob pointer matches no byte predicate (the resolver does not read blobs).

## Q: rows split across SSTs of one level
D9's point get seeks the row's markers and then the column in "one SST per deeper level". If a level splits a row across two SSTs, the marker and the column can be in different SSTs, and a GC that sees only part of a row could drop a family delete that still hides cells of the same row elsewhere in the level.

**Interim behavior (changed after review):** outputs are cut between rows once less than an eighth of the extent is left, so only a row larger than that is split. The picker takes inputs *and* the overlapping SSTs of the level below by row ranges, and expands both to a clean cut, to a fixpoint: an SST sharing an edge row with a chosen one comes along. A row's data in a level therefore always moves down together, and a bottommost run sees every SST of the output level holding its rows (regression test `a_row_split_across_bottom_ssts_moves_together`; the picker proptest generates shared edge rows and asserts clean cuts). The task's `range` stays `KeyRange::all()`, which covers the expansion. The engine's point get must consult every SST of a level whose range overlaps the row (normally one): D9's "one SST per deeper level" wording needs amending (coordinator).

## Q (confirmed): what the engine does with picker tasks
The picker does not know the tablet's row range or whether an SST is shared with a sibling after a split (D13).

**Interim behavior:** `pick` returns `range = KeyRange::all()` and one subrange; the engine narrows `range`/`subranges` to the tablet range and turns a `TrivialMove` of a shared SST into a `Rewrite`. `TrivialMove` and `Drop` need no job (the engine has the `SstMeta`s); a job given one finishes at once with an empty output. Subranges run one after another inside one job; splitting large tasks and running subranges in parallel is deferred (#35). `CompactionJob::new` debug-asserts that `range` and every subrange bound is an encoded row prefix.

## Q (confirmed): blob accounting in Phase 1
Value separation is Phase 2 (FORMAT §7), so Phase 1 writes no blob files.

**Interim behavior:** `blob_live_delta` records `-(16 + len)` per dropped put holding a blob pointer (the record header plus the value, matching `BlobWriter`'s byte count). `new_blob_files` and `dropped_blob_files` stay empty: the job does not know a file's current live bytes, so the engine decides when a file reaches zero. A `BlobGc` task finishes empty.

## Q (confirmed): point gets copy small values
A delete can follow the put it hides inside one `(column, timestamp)` group (it was committed earlier), so the resolver must read the whole group before returning the put, and `Cursor` cannot look ahead without moving.

**Interim behavior:** values up to 4 KiB are copied into a reused buffer while the group is read (`from_source == false`); a larger value is re-found with one seek and returned borrowed from the source (`from_source == true`). Nothing allocates per cell once the buffers have grown.

## Proposed decision: D22 amendment — time ranges on merge families apply to resolved versions
D22 pushes a scan's time range down to puts (operands and deletes always pass). For a family with a merge operator that can drop a counter's base while keeping its operands, so the read returns a wrong sum (for example base 100 at ts 10, operands at 20 and 30, range `[25, 40)`: pushdown returns 3, the counter is 103 at ts 30).

**Interim behavior (coordinator decision, to be numbered):** for a family with a merge operator the time range is not pushed down; it applies to *resolved* versions (`ResolveOptions::time_range`: a version, merged or not, is kept iff its timestamp is in range), after deletes, TTL and folding and before the value predicate and version limits. For a family without merge operands that equals pushdown, so `ResolveOptions::route_time_range(&mut filter, range)` sends the range to `ScanFilter::time_range` when there is no operator and to the resolver when there is one; the engine calls it when building a scan. A version whose fold fails but which is outside the range is not returned and so does not fail the read. Tests: `counter_time_range_applies_to_resolved_versions` and the time-range case of the resolver oracle.
