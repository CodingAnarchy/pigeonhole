# 0027: shard park and wake counters, and commit waits that park

**Status:** Approved (coordinator, 2026-10-10; #405, D198).

## Change

`pigeonhole-runtime`, additive:

```rust
impl<M: Send + 'static> Submitter<M> {
    /// `(parks, wakes)` of the target shard since the runtime started.
    pub fn idle_counts(&self) -> (u64, u64);
}
```

`pigeonhole-engine`, additive (`Metrics` is non-exhaustive):

```rust
pub struct Metrics {
    // ...
    /// Idle parks of the shards' engine-owned threads, and wakes of a parked shard,
    /// summed over shards.
    pub shard_idle: (u64, u64),
    /// Blocking waits for a commit's reply that parked their thread.
    pub commit_parks: u64,
}
```

`pigeonhole-bench`: `Stalls` gains `shard_parks`, `shard_wakes` and `commit_parks` (with serde defaults, so older result files still load). The stall table reports them per operation. `phdb-bench --shard-spin US` sets `Options::shard_spin`.

## Why

On a 4-core Linux runner, ycsb-a with one client is 2.1× RocksDB's p50 at the default 4 shards, and faster than RocksDB at 1 shard. The suspected cause: a client's commits spread over N shards, so each shard waits longer than its D198 spin between commits, parks, and the next commit pays a thread wakeup (20–30 µs on that VM). These counters measure that cause directly. They are the evidence D198 item 2 asks for before combining (C2) can come back to the owner, and #473's reference-machine run will record them.

## Semantics

- **`parks`** counts the times an engine-owned shard thread parked with nothing to do: after its idle spin, at the loop's idle park (`thread::park` or the deadline park). Waits on the thread's own I/O ring (#402) are not idle parks and aren't counted.
- **`wakes`** counts notifies (a submit, a task wake) that found the shard announced asleep and woke it: the producer's slow path, which already makes a syscall.
- **Application-owned shards** sleep in the application's loop, so their parks aren't counted; their wakes are (the registered wakeup).
- **`commit_parks`** counts blocking waits for a commit's reply (`Engine::commit`, `PendingCommit::wait`, check-and-mutate) that parked their thread at least once after the D198 client spin. A wait counts once, however often it re-parks. Async waits never park and aren't counted. Waits for visibility (the global watermark) aren't counted.
- **Cost:** relaxed `fetch_add`s on paths that park or wake a thread, never on a busy commit's path. The instruction shapes don't change.
- Readings are cumulative since open and relaxed: take two and subtract.

## Callers

- `pigeonhole-engine`: `Engine::metrics` fills the fields.
- `pigeonhole`: no change (`engine_metrics()` passes them through).
- `pigeonhole-bench`: reports them per operation; #473's runbook records them.
