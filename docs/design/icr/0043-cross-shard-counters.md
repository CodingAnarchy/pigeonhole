# 0043: `Metrics::cross_shard` counts cross-shard two-phase rounds and refused PREPAREs by reason

**Status:** Approved (owner, via the coordinator, 2026-10-13).

## Change

Additions only, in `pigeonhole-engine`. `pigeonhole` re-exports the new type beside `EngineMetrics` (both `#[doc(hidden)]` bench hooks, ICR 0015).

```rust
pub struct Metrics {
    // ...
    /// Cross-shard commits' two-phase rounds and refusals (ICR 0043).
    pub cross_shard: CrossShardMetrics,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CrossShardMetrics {
    pub rounds: u64,              // two-phase rounds decided, by every coordinator
    pub refused_rounds: u64,      // of those, rounds a participant refused
    pub refused_below_floor: u64, // PREPAREs below a timestamp the participant assigned (D11)
    pub refused_move_raise: u64,  // below a moved tablet's raised floor only (D11)
    pub refused_moved: u64,       // a tablet split, merge or move (D145)
    pub refused_room: u64,        // no arena room for the share (ICR 0042)
}
```

**Meaning.**
- `refused_rounds` over the cross-shard commits in an interval is the wasted rounds per commit. The commit retries each refused round, or ends with `Busy` at the write-stall timeout (D241).
- `rounds − refused_rounds` counts the rounds that committed, or failed for another reason (a conflict, an I/O error).
- A refused round holds one refusal per refusing participant, so the four reasons can sum to more than `refused_rounds`.

**Where they count.** Each shard has a `CrossShardCounters` block in its `ShardMetrics`: `#[repr(align(128))]`, so no other shard's counters share its cache lines. Only that shard's thread writes it, with `Relaxed` `fetch_add`:
- **The participant** counts its refusals in `route_member` (`ShardMetrics::count_refusal`, `#[cold]`).
- **The coordinator** counts each decided round in `on_prepared`.
- **No single-shard commit touches them,** so the commit path's shared lines are unchanged (D236).
- `Engine::metrics` sums them over shards.

**`refused_room`** has no writer until D245 adds `PrepareError::Room` (ICR 0042). D245 counts it in the same place.

**BelowFloor and move-raise.** Today's participant check is one comparison, `commit_ts < max(own floor, ts_raises)`. It is split as follows:
- below the own floor → `refused_below_floor`;
- otherwise → `refused_move_raise`.

D245 replaces the own-floor check with an exact, seqno-aware one, and keeps the move-raise check. Each refusal site calls `count_refusal` with its reason.

## Why

On the AX102 at 16 shards and 32 clients with half the commits cross-shard, tablet changes on cost 36× the throughput (11K against 401K ops/s). Scratch counters showed why:
- 3.1–3.5 two-phase rounds per cross-shard commit;
- more than 99.9% of the refusals were BelowFloor (#637);
- moves and packing barely registered.

Users and the bench need to see this without a scratch build. blob33's D246 gate row reads these counters, and D245's A/B uses them to show BelowFloor refusals going to about 0.

## Callers

- `pigeonhole-engine`:
  - `shard.rs`: `CrossShardCounters`, `CrossRefusal`, `ShardMetrics::count_refusal`, the round count in `on_prepared`;
  - `shard/tablets.rs`: the counts in `route_member`;
  - `engine.rs`: `Metrics::cross_shard`, `CrossShardMetrics`, the sum in `Engine::metrics`;
  - `lib.rs`: the export.
- `pigeonhole`: the `CrossShardMetrics` re-export, and the `engine_metrics` docs.
- Tests (`engine/tests/tablets.rs`):
  - a cross-shard commit no one refuses counts one round and no refusal, and a single-shard commit counts nothing;
  - a stale floor counts one `refused_below_floor` and one refused round;
  - repeated tablet changes count one or two `refused_moved` per refused round, and one more round to commit.
- Docs:
  - concurrency.md: the `ShardMetrics` rows and the counts;
  - the durability guide's "Cross-shard batches";
  - the agent reference;
  - changelog.
