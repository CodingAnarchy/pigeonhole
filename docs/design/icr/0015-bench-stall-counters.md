# 0015: stall counters for the bench (`Metrics` fields, `PagerStats` growths, `WalCounters`, `Pigeonhole::engine_metrics`)

**Status:** Approved (coordinator, 2026-10-09). Additive. (0014 is blob33's async read tier.)

## Change

1. **`pigeonhole-wal`:** `WalCounters` (`inline_grows`, `inline_rollover_syncs`; atomics) and `WalStream::counters() -> Arc<WalCounters>`. A metrics reader keeps the handle and reads it from any thread while the stream runs on its shard. `WalStream::inline_grows` and `inline_rollover_syncs` read the same counters, no longer the pool lock.
2. **`pigeonhole-pager`:** `PagerStats` (non-exhaustive) gains `growths` and `growth_nanos`: file growths since open, and the wall time each held the allocator across its `fallocate` and `sync_all` (#28, #182).
3. **`pigeonhole-engine`:** `Metrics` (non-exhaustive) gains:
   - `wal_inline_syncs`: the sum over shards of the streams' inline rollover syncs (#19, D30's exception);
   - `file_growths: (u64, u64)`: from the pager.

   Each shard's `WalCounters` handle is taken when its stream is attached at open, so a commit pays nothing for it.
4. **`pigeonhole`:** `#[doc(hidden)] Pigeonhole::engine_metrics() -> EngineMetrics` (a hidden re-export of the engine's `Metrics`), a bench hook like `shard_stats` (ICR 0010).
5. **`pigeonhole-bench`:**
   - `Runner::stalls()`, a provided method that defaults to `None`;
   - `Stalls` with `Stalls::between`;
   - `RunDetail::stalls`, `serde(default)`, so older result files still load.

   The Pigeonhole runner reports the measured phase's write stalls, flushes, compactions, unpin passes, inline WAL syncs and file growths, and the markdown summary prints them as a table.

## Why

Phase 3's write-path work (#64, #19, #28/#182, #175) needs to know which of these stalls happen during a run, and how often, before deciding what to fix. Two of the counters did not exist, and the public crate could not reach the engine's.

## Callers

- `pigeonhole-engine` (`Engine::metrics`, stream attachment at open).
- `pigeonhole` (the hook).
- `pigeonhole-bench` (the runner and the report).
- Existing users of `WalStream::inline_grows` and `inline_rollover_syncs` (the wal tests) are unchanged.
