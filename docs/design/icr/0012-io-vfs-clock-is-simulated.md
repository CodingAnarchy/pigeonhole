# 0012: `pigeonhole-io` `Vfs::clock_is_simulated`

**Status:** Proposed (issue #263). Implemented in the PR that closes #263; it needs approval before that PR merges.

## Change

```rust
pub trait Vfs {
    // ...existing methods...

    /// Whether the clocks are simulated: they move only when the program moves them.
    fn clock_is_simulated(&self) -> bool { false }
}

impl Vfs for SimVfs {
    fn clock_is_simulated(&self) -> bool { true }
}
```

This is additive: a provided method, so every existing implementation compiles unchanged and reports a real clock.

## Why

The engine decides that a clock has stopped (the frozen-clock fallbacks of D119, D126, D161 and D171) with a poll count: a timer that reads an unchanged clock 1024 times in a row gives up. A real clock that ticks coarsely (`CLOCK_MONOTONIC_COARSE`, a jiffies clocksource, a cached clock in an application-owned runtime) reads the same for milliseconds, so it was taken as stopped. Then a commit waiting for arena room was refused with `Busy` at once, an L0 stall stopped pacing writers, and flush retries capped at 4 (review 1-2 F7). The test in #263 measured a refusal after 1.6 ms where the timeout was 300 ms. Only the `Vfs` knows whether its clock is simulated, so it says so.

## Semantics

- **Real clock (the default).** `ClockTimer` sleeps until its deadline (`TaskPoll::SleepUntil`) and never gives up. The runtime never wakes a sleeper early because two readings matched. None of the frozen-clock fallbacks apply.
- **Simulated clock.** Unchanged. A timer that sees the clock stand still for `STALL_TIMER_FROZEN_POLLS` polls gives up and the fallbacks apply. The runtime wakes sleepers early when the clock has not moved since it went idle.
- **Wrappers.** A `Vfs` that wraps another and keeps its clocks forwards the method. One that substitutes a real clock (the test VFSes on `Instant`) keeps the default.

## Callers

- `pigeonhole-engine`: `ClockTimer::run` (shard.rs).
- `pigeonhole-runtime`: the shard loop's `run_once` and the compaction thread loop (`sched.rs`), for the early wake of sleepers.
- Test VFS wrappers forward it: `engine/tests/{close_failures, deferred_waits, gate, milestone_b, network_fs, sync_faults, tablets}`, `pager/tests/{async_commit, sync_faults}`, `wal/tests/{pread, side_sync}`. The `idle_cpu` wrappers in engine and pigeonhole run on a real clock and keep the default.
