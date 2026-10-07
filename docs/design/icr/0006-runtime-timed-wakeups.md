# 0006: `pigeonhole-runtime` timed wakeups — `TaskPoll::SleepUntil` and `next_deadline`

**Status:** Approved (coordinator, 2026-10-07; issue #89). Implemented in the PR that closes #89.

## Change

```rust
pub enum TaskPoll {
    Pending,
    Blocked,
    /// Nothing to do until the VFS clock reaches this deadline (or the task's waker fires).
    SleepUntil(u64),
    Done,
}

impl<H: ShardHandler> ShardDriver<H> {
    /// The earliest deadline of a background task sleeping on this shard.
    pub fn next_deadline(&self) -> Option<u64>;
}

impl EngineShard {          // pigeonhole-engine, application-owned mode
    pub fn next_deadline(&self) -> Option<u64>;
}
```

Semantics:
- A task returning `SleepUntil(d)` runs again once `Vfs::monotonic_nanos() >= d`, or earlier when its `TaskWaker` is woken.
- **Engine-owned mode:** a shard (or compaction-pool thread) with only sleeping tasks parks until the earliest deadline or the next message, with no polling. Until the clock has shown that it keeps pace with real time, the thread re-checks every 10 ms. After that it re-checks every 1 s, as a safety net. The early re-checks are needed because a simulated clock moves without waking anyone.
- **Application-owned mode:** sleeping tasks are not "work that remains". `ShardDriver::run_once` returns `false` when only they are left, and the application calls it again by `next_deadline()` or when its wakeup fires.
- **Frozen clock:** if a pass finds the clock reading unchanged since the shard last went idle, the runtime runs sleeping tasks early so they can notice a frozen clock (D126). A sleeping task must therefore tolerate running before its deadline.
- **Determinism:** under `SimVfs`, deadlines are on the simulated clock and application-owned runs stay deterministic.

## Why

Before this change, the only way to wait for a time was to return `Pending` on every slice. The engine's clock timers did exactly that: the L0 stall refill, the arena-room timeout, and the compaction backoff (#84, D126). So a shard in a stall or a 1–60 s backoff burned a full core on a real clock. Measured on `main`: 3 s of CPU in 3 s of backoff. With this change: under 1%.

## Callers

- `crates/runtime`: `sched.rs` (scheduler, pool threads), `lib.rs` (shard loop, `ShardDriver`). `TaskPoll` is matched only inside the runtime, so adding a variant breaks no other crate. The crate is not published yet, so CI's semver check skips it.
- `crates/engine/src/shard.rs`: `ClockTimer` probes the clock, then sleeps with `SleepUntil` once it has seen the clock move. A cancel wakes it at once.
- `crates/engine/src/engine.rs`: `EngineShard::next_deadline`.
- `crates/pigeonhole/src/db.rs`: the frozen public `Shard::run_once` keeps returning `true` while a background timer is pending, as it effectively did before. An application that waits only on `set_wakeup` therefore still runs it. A public `next_wakeup` would need its own ICR (issue #92). Superseded for the public crate by ICR 0007.
