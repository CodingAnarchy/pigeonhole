# 0064: the drift gauge, `EngineMetrics::{default_ts_lead_micros, max_default_ts_lead_micros}`

**Status:** Approved (owner, via the coordinator, 2026-10-22; D271, #886). Additive.

## Change

**`pigeonhole`:** two new public fields on `EngineMetrics`, which is `#[non_exhaustive]`:
- `default_ts_lead_micros`: how far the largest default timestamp handed out (D271's `H`) is ahead of the clock now, in µs. It saturates, so it reads 0 while the clock leads.
- `max_default_ts_lead_micros`: the largest such lead at any commit since open.

**`pigeonhole-engine`:** `Metrics::default_ts_lead_micros: (u64, u64)` holds the pair (now, largest), mapped to the two fields above.

## Why

D271 hands out every default timestamp from one allocator. Default timestamps run ahead of the wall clock only when:
- one shard commits more than one group in a µs; or
- a cross-shard decision and a commit after it fall in the same µs (the decision flag).

That drift moves TTL timing and how close default timestamps stay to `now`, so it is reported rather than assumed small (#854 §2).

The engine tracks the largest lead in an atomic beside the allocator, written only when it grows. The current lead is computed when metrics are read.

## Callers

- **`pigeonhole-engine`, `clock.rs`:** `Reservation::{reserve_group, reserve_decision}` note the lead; `Reservation::lead` reads it.
- **`pigeonhole-engine`, `engine.rs`:** `Engine::metrics` fills `Metrics::default_ts_lead_micros`.
- **`pigeonhole`, `metrics.rs`:** `From<pigeonhole_engine::Metrics> for EngineMetrics` maps it.
- **Tests:** `clock.rs` unit tests; `tablets.rs` `d142_sweep` (a lead on the frozen clock).
