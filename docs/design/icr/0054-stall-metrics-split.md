# 0054: room stalls and L0 pacing, counted apart

**Status:** Approved (owner, via the coordinator, 2026-10-21; #724 area 3, PR 2). Additive: the old fields stay as the sum.

## Change

In `pigeonhole-engine`:
- `Metrics::room_stalls: (u64, u64)`: waits for memtable arena room (D124), as a count of waits begun and the nanoseconds spent in them, summed over shards.
- `Metrics::pacing_stalls: (u64, u64)`: L0 pacing spans (D119), counted the same way.
- `Metrics::stalls` stays, as their sum. `Metrics` is `#[non_exhaustive]`, so the new fields break no caller.
- Internally, `ShardMetrics::stalls` and `stall_nanos` become `room_stalls`, `room_stall_nanos`, `pacing_stalls` and `pacing_stall_nanos`. The tablet balancer's stall term (D255) reads their sum, as it read the old counter.

In `pigeonhole`:
- `EngineMetrics::room_stalls`, `room_stall_nanos`, `pacing_stalls` and `pacing_stall_nanos`, from the engine's fields. `EngineMetrics` is `#[non_exhaustive]`.
- `write_stalls` and `write_stall_nanos` stay, as the sum.

In `pigeonhole-cli` (`\metrics`) and `pigeonhole-bench` (not published):
- The shell's `\metrics` lists `room_stalls` and `pacing_stalls` after `write_stalls`.
- `Stalls` gains the four fields (`#[serde(default)]`, so older results still load).
- The report's stall columns read "total (room / pacing)".

## Why

The two stalls have different causes and different fixes:
- **A room stall** is arena pressure: too small an arena, flushes that can't keep up, or snapshots holding flushed memtables.
- **A pacing stall** is compaction debt.

One counter for both meant a gate report couldn't say which one a run paid for (#724 area 3). The owner asked for the old fields to stay, so the change is additive.

## Callers

- `pigeonhole-engine`: `engine.rs` (`Metrics`, `metrics()`); `shard.rs` (`ShardMetrics`, `end_pacing`, `end_room_wait`, `stalled`, `wait_for_room`); `shard/tablets.rs` (the balancer's stall term); `tests/milestone_b.rs` (a pacing test and a room test each assert their own counter, and that `stalls` is the sum).
- `pigeonhole`: `metrics.rs` and its unit test.
- `pigeonhole-cli`: `shell/mod.rs`; `tests/snapshots/shell_meta.snap`.
- `pigeonhole-bench`: `report.rs` (`Stalls`, `Stalls::between`, the stall table); `runners/pigeonhole.rs`.
- Docs: `docs/guide/agent-reference.md`, `docs/guide/errors.md`, the changelog.
