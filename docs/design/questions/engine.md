# Engine questions

## Q: Should a bottommost compaction's purge run under the D191 guard, with a fallback after repeated voids? (#316, #328)
D70 bounds a bottommost purge by `GcPolicy::min_ts_above`, which the compaction samples when it plans. Nothing re-checked that sample when the compaction installs. A write committed in between at an explicit timestamp below the sample, and among the inputs' timestamps, can make the install change a read with no write in between:
- a `delete_cell` of the newest version exposes an older version that the compaction drops;
- a `put` below a column delete appears once the compaction purges the delete.

D191 already guards a flush's purge with a void/wait handshake (`IN_FLIGHT` / `VOIDED` / `INSTALLING`).

**Interim behavior (proposed refinement of D70 and D191):**
- **When a guard is registered:** a `Rewrite` or `BlobGc` compaction that is bottommost, has `min_ts_above > 0`, and is not in a counter family (D179 purges by seqno) runs under a guard.
- **What touches it:** a member touches the guard when it writes in the family (put, merge, or any delete) at a timestamp `ts` with `ts < min_ts_above` and `ts <= max_ts`, where `max_ts` is the newest timestamp among the inputs.
  - Writes at or above the bound are ones the purge already allows for.
  - A write above every input is a newer version, a delete that hides no input, or a delete that hides all of them. Each reads the same with or without the purge.
- **Writes without a timestamp:** a default timestamp counts at its lower bound.
  - For a single commit, that is `floor + 1`, or its preset when the preset still fits the floor.
  - For a share, it is the share's commit timestamp. With tablet changes off, that can be below the participant's floor, because D11's `BelowFloor` refusal covers only tablet changes on.
- **Admission:**
  - While the guard is in flight, a touching member voids it.
  - While its commit installs, the member waits, and a PREPARE waits too, as in D191.
- **Install:** the compaction's commit claims the guard in its catalog closure (from `IN_FLIGHT` to `INSTALLING`).
  - A voided commit is refused (`Busy`), and its outputs are freed.
  - The job also checks the guard before each slice and stops early once it is voided.
  - A voided compaction is not a failure: there is no backoff, and the slot is planned again at once with a fresh bound.
- **Fallback:** after `PURGE_VOID_LIMIT` (3) consecutive voids of a slot, the next compaction of that slot runs with `min_ts_above = 0`. That run does no bottommost purge and needs no guard, so it installs. The purge is left to a later compaction, and the count resets when a compaction of the slot installs.
  - This keeps a backfill written newest to oldest from starving a compaction that relieves an L0 stall.
