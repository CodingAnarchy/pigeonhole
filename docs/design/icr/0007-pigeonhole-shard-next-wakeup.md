# 0007: `pigeonhole::Shard::next_wakeup`, and `run_once` idle with only timers left

**Status:** Approved (coordinator, 2026-10-07; issue #92). Implemented in the PR that closes #92.

## Change

```rust
impl Shard {
    /// How long until background work on this shard is due, or `None`.
    pub fn next_wakeup(&self) -> Option<Duration>;
}
```

`Shard::run_once` keeps its signature. Its result changes for one case: background work that is waiting for a time no longer counts as work that remains. That covers a write stall's pacing, a wait for memtable room, and a failed compaction's retry, which backs off from 1 s, doubling up to 60 s (D126). With only such work left, `run_once` returns `false`, and `next_wakeup` says when it is due. The `set_wakeup` callback still fires only when work arrives, never when background work falls due.

The documented loop for each shard:
1. Call `run_once` until it returns `false`.
2. Sleep until the `set_wakeup` callback fires or `next_wakeup` passes, whichever comes first.
3. Repeat.

## Why

ICR 0006 (#89) gave the runtime timed wakeups. Engine-owned shards now park through those waits, but the public application-owned API had no way to learn the deadline. To keep loops that sleep only on `set_wakeup` correct, `run_once` kept reporting pending timers as work, so the documented loop spun through every stall and backoff. Measured: 2.19 s of CPU in 3 s of a compaction backoff, against under 1% with this change.

## Compatibility

Additive: one new method. The changed `run_once` result is safe for loops that follow the old documentation (call `run_once` while it returns `true`, otherwise wait for the wakeup or keep polling). Only a loop that sleeps on the wakeup alone, ignoring `next_wakeup`, would now run background timers late. Such a loop runs them at the next arriving work instead of spinning until they fire. The crate is not published yet, so there is no semver baseline.

## Callers

- `crates/pigeonhole/src/db.rs`: `Shard::run_once` and `Shard::next_wakeup`, plus the loop documented on `Pigeonhole::open_application_owned` (doctest).
- `crates/pigeonhole/tests/idle_cpu.rs`: the documented loop idles under 1% CPU through a compaction backoff, and the retry fires on schedule.
- `docs/guide/agent-reference.md`.
- No other crate drives a public `Shard`; the engine's `EngineShard::next_deadline` (ICR 0006) is the source.
