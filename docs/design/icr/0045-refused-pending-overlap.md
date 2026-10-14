# 0045: `CrossShardMetrics::refused_pending_overlap`; a PREPARE overlapping an undecided share is refused, not a `Conflict`

**Status:** Approved (owner, via the coordinator, 2026-10-13; D249).

## Change

**Public, additive:** one field in `pigeonhole-engine`'s `CrossShardMetrics` and in `pigeonhole`'s `CrossShardMetrics` (its own copy since ICR 0044, with its `From`). Both are `#[non_exhaustive]`.

```rust
pub struct CrossShardMetrics {
    // ...
    /// PREPAREs refused because they overlapped a row of an undecided share another commit
    /// prepared there (D249). Retried once it is decided; not a conflict.
    pub refused_pending_overlap: u64,
}
```

**Internal** (`pigeonhole-engine`, crate-private):
- `PrepareError::PendingOverlap` and `CrossRefusal::PendingOverlap`;
- `ShardMsg::Wake { from, reason: WakeReason::{Room, Overlap} }`, replacing D245's `RoomFreed`, plus `ShardMsg::Released`;
- `Decide` gains the commit's `priority` and `last`;
- `CommitReq` carries a boxed `Retried` (preset, first seqno, previous participants) in place of its inline `Option<Preset>`, so the queued message shrinks.

**Behaviour:**
- A cross-shard PREPARE that overlaps another commit's undecided share is refused and retried, instead of failing the commit with `Conflict`. One that overlaps only an earlier member of its own group waits for the next group, as conditional singles do.
- `Conflict` reaches the application only from validation (a commit to a read key above the snapshot) or a failed predicate.
- A commit that keeps meeting overlaps gives up with `Busy` at the write-stall timeout (D241).

## Why
blob33's measurement (`.coord/inbox/done/20261013-184539-blob33.md`):
- cross-shard transfers conflicted 5–16× more often than same-shard ones;
- under `GroupSync`, 85 conflicts per transfer at 500K accounts and 16 clients, each failing in 10 µs.

Most were not conflicts at all: the overlapped share was still undecided, often aborted later, and the application retried in a tight loop.

## Callers
- `pigeonhole-engine`:
  - `shard.rs`: `run_group`'s conditional branch, `on_prepared`, `on_applied`, `on_decide`, `start_coordination`;
  - `shard/tablets.rs`: `refuse_for_overlap`, `release_overlaps`, `release_overlaps_behind`, `on_overlap_cleared`, `retry`, routing, `run_retries`;
  - `engine.rs`: request literals and the metrics sum.
- `pigeonhole`: `metrics.rs` (the field, the `From`, and its test).
- **Tests** (`crates/engine/tests/tablets.rs`):
  - `a_cross_shard_transaction_overlapping_an_undecided_share_retries_instead_of_conflicting`;
  - `a_cross_shard_transaction_still_conflicts_with_a_write_after_its_snapshot`;
  - `two_crossing_cross_shard_prepares_neither_deadlock_nor_livelock`;
  - `crossing_prepares_both_refused_resolve_older_first_on_a_frozen_clock`;
  - `crossing_prepares_driven_to_recross_every_retry_still_resolve_older_first` and `a_younger_commit_refused_behind_an_older_one_waits_for_the_older_ones_next_attempt` (ctr274), each of which fails with older-first disabled.
- **The bench** (blob33): `refused_pending_overlap` in the report rows.
