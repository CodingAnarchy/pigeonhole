# 0087: The frozen metrics are the ones a user acts on; the engine's mechanism counters move to a hidden `internal_metrics()`; `WalFile` positions become `WalPosition`

**Status:** Approved (owner via the coordinator, 2026-10-28; #695, API audit 2a, 2b and 2d, c6480352190). perf287 agreed the set with two changes (c on #695, 2026-10-28T21:00Z): `background_{fallback, room}_take_backs` stay public, and `unpinned_reason` keeps its "wording may change" sentence. Breaking before the 1.0 freeze. The engine still counts everything, and no behaviour changes.

## Change
**Stay public, frozen in 1.x.** Every struct stays `#[non_exhaustive]`, so a minor release may add fields.
- **`EngineMetrics`:**
  - `commits(Durability) -> CommitStats` (percentiles within a factor of 1.5, so a finer histogram later stays compatible);
  - `flushes`, `flush_failures`, `compactions`, `compaction_failures`, `compactions_reclaimed`, `background_fallback_take_backs`, `background_room_take_backs`;
  - `write_stalls`, `write_stall_nanos`, `room_stalls`, `room_stall_nanos`, `pacing_stalls`, `pacing_stall_nanos`;
  - `block_cache_{hits, misses, capacity_bytes, pinned_bytes, pinned_warnings}`;
  - `file_growths`, `file_growth_nanos`, `async_sync_reads`, `shard_busy_nanos`;
  - `reader_slots`, `reader_slots_active`, `reader_slots_reclaimed`, `reader_pin_seqno_lag`;
  - `threads_started`, `threads_pinned`, `unpinned_reason` (its wording is not part of the promise);
  - `transactions`, `transaction_conflicts`, `transaction_retries`.
- **`CrossShardMetrics`:** `rounds`, `failed_fast`.
- **`IoRings`:** `rings`, `attach_failures`, `inline`. `inline` is frozen as "I/O completed synchronously on the calling thread instead of through a ring", so D292's page-cache hits can count toward it, or not, compatibly.
- **`WalFile`:** `stream`, `path`, `len`, `records`, `segments`, `segment_size`, plus `checkpoint` and `end` as a new `WalPosition { epoch: u32, offset: u64 }` (`#[non_exhaustive]`), replacing the tuples `(u32, u32)` and `(u32, u64)`.

**Move to `Pigeonhole::internal_metrics() -> Vec<(&'static str, u64)>`.** It is `#[doc(hidden)]` and needs ICR 0085's `__unstable-hooks` feature. Names and their presence may change in any release, and readers treat a missing name as 0 or absent.
- **WAL:** `wal_unpin_passes`, `wal_unpin_flushes`, `wal_inline_syncs`, `wal_rollover_blocks`.
- **File growth ahead:** `file_growths_ahead`, `file_growth_ahead_nanos`.
- **Default timestamps:** `default_ts_lead_micros`, `max_default_ts_lead_micros`.
- **Shard threads:** `shard_parks`, `shard_wakes`, `shard_pool_busy_nanos`, `shard_spin_time_nanos`, `shard_commit_inflight_nanos`, `commit_parks`.
- **Reader view lag:** `reader_pin_view_lag`.
- **Visibility:** `visibility_waits`, `visibility_waiters_ahead`, `visibility_wake_passes`, `visibility_woken`, `visibility_contended`.
- **Cross-shard:** `cross_shard.refused_rounds`, `cross_shard.refused_moved`, `cross_shard.refused_room`, `cross_shard.refused_pending_overlap`, `cross_shard.share_waits`, `cross_shard.share_wait_nanos`, `cross_shard.chained`, `cross_shard.wounds`, `cross_shard.max_attempts_blind`, `cross_shard.max_attempts_reading`.
- **io_uring:** `io_rings.pooled`.
- **D292's inline page-cache reads:** `nowait.on`, `nowait.hits`, `nowait.misses`, `nowait.miss_nanos`, `nowait.refused`, `nowait.breaks`.

**Removed outright.** `CrossShardMetrics::refused_below_floor` and `refused_move_raise`, which were deprecated and always 0 since 0.4.0.

**Hidden, not gated.** `WalFile::end_slot` becomes `#[doc(hidden)]` but stays a field, because `phdb check` prints it and the cli doesn't turn the hooks feature on.

## Why
`#[non_exhaustive]` lets a 1.x release add a field but never remove one. The moved counters name mechanisms that later work is likely to change or remove: #724's share reports, D289's drivers, D292, the WAL's unpin passes and the visibility waiters. The public set is what an application can act on.

## Callers changed
- **The bench** (`runners/pigeonhole.rs` `stalls()` and `reader_process.rs`) reads the moved counters by name through `internal_metrics()`. A missing name reads as 0, and the report's JSON keys are unchanged, so baselines and gate scripts read the same files.
- **Tests:** api.rs and readers.rs read them through the hook, and the metrics unit test checks every name once.
- **The cli** builds its `check` JSON from `WalPosition`; its output is unchanged.
- **Docs:** the agent reference, durability.md and readers.md.
