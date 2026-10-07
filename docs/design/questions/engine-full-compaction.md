# Engine: full compaction with tablet changes (issue #94)

## Q: What does `Engine::compact` guarantee while tablets split, merge and move?
`Engine::compact` sends one `CompactAll` to every shard; each shard compacts the slots it owns and replies. With tablet changes on, the balancer kept moving and merging tablets meanwhile: a tablet could leave a shard before that shard's round reached it and arrive at a shard whose round was over, so it was never compacted (seed 13 of `results_are_identical_across_shard_counts_with_tablet_changes`). And a slot holding one SST above the last level was moved there without a rewrite, keeping deletes that a rewrite of the same rows purges (D74). How many SSTs a slot holds depends on when splits and moves flushed it, which depends on the shard count (seed 106).

**Interim behavior (tablet changes on only; off, nothing changes):**
- While a full compaction runs, the balancer starts no change (`Shared::full_compactions`). A round during which a tablet change finished or was given up (`Shared::tablet_epoch` moved) is followed by another round, until one completes with no change. Changes requested explicitly (test hooks) still run; they only add rounds.
- `plan_full` rewrites a lone SST above the last level instead of moving it, so every slot's full compaction purges what a bottommost compaction may.

Proposed decision: a full compaction compacts every slot that exists when it is called, whatever tablets do meanwhile, and leaves each slot as one rewritten run at the last level. Whether the lone-SST rewrite should also apply with tablet changes off (it costs one rewrite per slot with a single L0 SST, and makes `compact` purge consistently) is for the coordinator; this PR keeps it gated, as D129 requires.
