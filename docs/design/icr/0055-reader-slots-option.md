# 0055: `Options::reader_slots`, and the writer's reader metrics

**Status:** Approved (owner, via the coordinator, 2026-10-21; #693, D1 and D3). Additive.

## Change

In `pigeonhole`:
- `Options::reader_slots(self, n: u32) -> Self`: how many reader handles can attach at once. Default 128, clamped to 1..=65,536.
- `EngineMetrics` (`#[non_exhaustive]`) gains:
  - `reader_slots_active` and `reader_slots` (gauges);
  - `reader_slots_reclaimed` (a counter);
  - `reader_pin_view_lag` and `reader_pin_seqno_lag` (gauges: how far the oldest reader snapshot is behind the current view and the visible seqno; 0 with none).

In `pigeonhole-engine`: `Metrics` gains `reader_slots`, `reader_slots_reclaimed` and `reader_pin_lag`. In `pigeonhole-shm`: `ShmRegion::reader_slot_usage()`.

The engine already had `EngineOptions::reader_slots`, used to size the region and the footprint check. The public crate only passes the value through.

**The default moves from 126 to 128** (owner, 2026-10-21): in `ShmConfig`, `EngineOptions` and the new option. Nothing recorded why it was 126 (it dates from #11). Slots are 64 bytes and the slot area is padded to the arenas' 2 MiB alignment, so two more cost nothing; a test pins that the region size is the same. Compatibility is unaffected: a reader takes the slot count from the region header the writer built, never from its own configuration.

## Why

- **D3:** the slot count bounds concurrent readers, and one more fails `NoReaderSlot` at once. An application with more than the default's reader handles (one per worker in a pre-forked server, for example) had no way to raise it.
- **D1:** a reader holding one snapshot for a long time stalls the writer into `Busy`, by design. The writer had no way to see that coming. The lag gauges show it, and the slot counts show attachment and reclamation (#833).

## Limits

The upper bound, 65,536 slots (4 MiB of the shared-memory file), keeps the writer's 250 ms dead-reader scan cheap. The count is fixed for a writer's lifetime; a new writer with another count builds its region with that many.

## Callers

- `pigeonhole`:
  - `options.rs` (the setter and the engine mapping);
  - `metrics.rs` (the fields);
  - `tests/readers.rs`:
    - `reader_slots_bound_attached_readers_and_one_more_fails_at_once`;
    - `a_killed_readers_slot_comes_back_and_is_counted`;
    - `the_oldest_reader_snapshots_lag_is_reported`.
- `pigeonhole-engine`: `engine.rs` (`Metrics`, `Engine::metrics`) and `shard.rs` (`Shared::reader_slots_reclaimed`).
- `pigeonhole-shm`: `region.rs` (`reader_slot_usage`).
- Docs:
  - the new guide page `docs/guide/readers.md`;
  - the agent reference, errors.md, concepts.md;
  - the changelog.
