# Engine questions (Phase 2)

## Proposed decision: the stopped-clock fallbacks apply only to a simulated clock (#263; amends D126, refines D161 and D171)
D126's fallbacks for a clock that does not move (admit writers when no compaction can end an L0 stall, refuse a hopeless room wait at once, cap flush retries) were triggered by a poll count. A timer that read the same clock value 1024 times gave up. A coarse real clock reads the same for milliseconds and got the simulator's fallbacks on a moving clock (review 1-2 F7).

**Interim behavior (ICR 0012):**
- The added provided method `Vfs::clock_is_simulated()` is `false` by default and `true` for `SimVfs`.
- On a real clock, `ClockTimer` sleeps to its deadline and never gives up, and the runtime never wakes sleepers early on matching readings. Waits end by their timers and timeouts, as D124 and D161 describe for a moving clock.
- On a simulated clock nothing changes: the poll count still detects a stopped clock, and D126, D161 and D171 apply as before.
- Test wrappers around `SimVfs` forward the method; wrappers that substitute a real clock keep the default.
- Regression test: `engine/tests/coarse_clock.rs` uses a real clock in 4 ms steps. A commit waiting for room that snapshots hold gets `Busy` only after the 300 ms stall timeout; with the old behavior it was refused after 1.6 ms.
