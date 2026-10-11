# 0023: visibility-waiter counters in `Metrics`

**Status:** Approved (coordinator, 2026-10-10; #154, D204).

*Amended by #134 step 2 (#528; approved, coordinator, 2026-10-11): `visibility_wakes.0` counts only the wake passes that reached a registered seqno. A watermark publish below every waiter's seqno no longer takes the list's lock, so it is not a pass, and `visibility_contended` counts fewer acquisitions. The list also holds shards with a deferred freeze: their registrations and kicks are not counted in `visibility_waits.0` or `visibility_wakes.1`, though their entries count as scanned and their lock acquisitions as contended. So `visibility_wakes.1 <= visibility_waits.0` still holds.*

## Change

`pigeonhole-engine`, additive (`Metrics` is `#[non_exhaustive]`):

```rust
pub struct Metrics {
    // ... (existing fields)
    /// Async commit polls that registered on the global visibility list, and the list
    /// entries those registrations scanned for a duplicate.
    pub visibility_waits: (u64, u64),
    /// Wake passes over that list, and the waiters they woke.
    pub visibility_wakes: (u64, u64),
    /// Registrations and wake passes that found the list's lock held.
    pub visibility_contended: u64,
}
```

`pigeonhole` re-exports `Metrics` as `EngineMetrics` (ICR 0015), so `Pigeonhole::engine_metrics` carries the new fields with no new method.

## Why

#154 needs to know whether the global visibility watermark (D19) limits write scaling before anyone reworks it. The scaling gate on the reference machine (#405) reports waits per operation at each N. They're new `Metrics` fields rather than a new method or type, to keep the API surface small (coordinator, 2026-10-10).

## Semantics

Cumulative since open. The counters move only on the async waiter's slow path: a relaxed atomic add per registration, wake pass and woken waiter. Taking the lock uses `try_lock` first, so contention is counted without timing anything. A waiter is woken at most once, so `visibility_wakes.1 <= visibility_waits.0`.

## Callers

- `pigeonhole-engine`: `shard.rs` (`VisibilityWaiters` counters and `lock`), `engine.rs` (`Engine::metrics`).
- `pigeonhole-bench`: `Stalls` gains the five counts; the inline scaling runner records them; `Scaling::visibility_waits_per_op`; `baselines/phase3-io/scaling.sh`.
