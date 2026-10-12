# 0033: shard busy time (`busy_nanos`)

**Status:** Approved (coordinator, 2026-10-11; D217, #478).

## Change

`pigeonhole-runtime`, additive:

```rust
impl<M> Submitter<M> {
    /// Nanoseconds the target shard's engine-owned thread has spent in its work turns since
    /// the runtime started: handling messages and running its tasks, not spinning or parked.
    /// An application-owned shard counts nothing.
    pub fn busy_nanos(&self) -> u64;
}
```

`pigeonhole-engine` (re-exported by `pigeonhole`), additive fields (both structs are `#[non_exhaustive]`):

```rust
pub struct ShardStats { /* ... */ pub busy_nanos: u64 }   // per shard, cumulative
pub struct Metrics { /* ... */ pub shard_busy_nanos: u64 } // summed over shards
```

`pigeonhole-bench`: each per-shard entry of a run's JSON (`ShardShare`) gains `busy_nanos` for the measured phase (`#[serde(default)]`, so older JSON still reads).

## Why

D217 packs tablets onto fewer shards when writes are light (#478). It needs to know how busy each shard is. Write rates alone don't say: their cost varies with commit size, flushes and compaction, and a rows-per-second capacity would need calibrating per machine. Busy time measures it directly. PR 1 adds only the counter, so D217's targets (pack to `ceil(B / 0.5)` shards, spread past 0.75) can be tuned from measured values of B, the summed busy fraction, before the policy lands.

## Semantics

- **Counted in `shard_main` only** (engine-owned shard threads): each loop turn reads the monotonic clock before `run_once`, which the turn's deadline already does, and once after, then adds the difference. The D198 spin, ring waits and parks happen outside `run_once` and count nothing. A turn woken only by a task deadline with nothing to do counts its few hundred nanoseconds.
- **Not counted:** application-owned shards (`ShardDriver::run_once` is untouched, so the D193 instruction ceilings measured there don't move) and the runtime's pool threads.
- **One writer** (the shard thread, `fetch_add` Relaxed), any reader (load Relaxed). A reading taken during a turn lags that turn. Inventoried in `concurrency.md` (`Signal::busy`).
- **Cost:** one extra monotonic clock read per turn (a vDSO call), and a turn handles up to a batch of messages.

## Callers

- `pigeonhole-engine`: `Engine::shard_stats` fills `ShardStats::busy_nanos`; `Engine::metrics` sums `Metrics::shard_busy_nanos`.
- `pigeonhole`: `shard_stats()` and `engine_metrics()` pass them through.
- `pigeonhole-bench`: `ShardShare::busy_nanos` from `shard_stats()`, subtracted over the measured phase (the inline driver's application-owned shards report 0).
