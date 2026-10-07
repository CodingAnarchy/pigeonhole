# Engine: a compaction and the shares a shard holds prepared (#132)

## Q: Does a prepared, undecided cross-shard share count as above a compaction's inputs?
D70 takes `GcPolicy::min_ts_above` from the upper SSTs and the slot's memtables. A participant holds a cross-shard share between PREPARE and the decision outside the memtables, and a commit applies it under the commit's seqno, which can be below every entry already in the memtables. A bottommost compaction that ran in that window purged a row delete above the share's explicit timestamp, and the share appeared once applied (seed 183 of `tablet_changes_with_a_changed_shard_count`). Tablet changes are not needed: two tables on two shards take the same path.

The test hook's `CompactionRecord::max_seqno` had the same gap: just below the memtables' oldest seqno, which can be above `visible`, so the model counted the share as an input.

**Interim behavior:**
- `min_ts_above` also folds in the smallest cell timestamp (explicit, or the commit's) of every share the shard holds prepared that writes the slot's family, whatever its tablet (routing may change before the decision). An aborted share only makes the bound lower than needed.
- `CompactionRecord::max_seqno` is never above the visible seqno. A memtable freezes only once its seqnos are visible, so no input is above it; a seqno past it may be a commit not applied here yet (prepared, or its PREPARE still on the way). One whose PREPARE arrives after the compaction starts counts as a later write (D74): it may appear below a delete the compaction purged, as any later write with an older explicit timestamp may.
