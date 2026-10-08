# Engine questions (Phase 2)

## Proposed decision: a room wait's re-check timer can tell the clock stopped (#244; refines D126, D161)
D126 refuses a commit waiting for arena room with `Busy` on a frozen clock once nothing in flight can free room, since a stopped clock never reaches the stall timeout. "Frozen" was read only from the wait's timeout timer, which gives up after polling an unchanged clock.

The public model suite (`quiet_runs_with_tablet_changes_off_match_the_model`, seed 53, on CI in a 50-seed process) hung 7 times in 20 runs. The harness advances the simulated clock from its own thread, then blocks in a `GroupSync` commit at op 391 whose room snapshots hold. The CI trace (via the new watchdog) shows the mechanism:
1. The timeout timer saw the clock move while the wait began, and slept towards its 30 s deadline, which the stopped clock never reaches.
2. Every ~3 ms the 1 ms re-check timer (D161) gave up on the stopped clock and kicked the shard, which retried, found no room and armed a new re-check.
3. The shard therefore never went idle, so the sleeping timeout timer was never polled again to notice, and the commit never resolved.

**Interim behavior:**
- `RoomWait::stopped(now)`: the clock stopped if either the timeout timer or the re-check timer gave up on this reading. The room wait and the starved-freeze wait both use it, so the case above takes D126's frozen-clock path (refused with `Busy`) at the first re-check that gives up.
- On a moving clock nothing changes, because a re-check timer never gives up there (`a_reader_pin_waits_for_the_stall_timeout_on_a_moving_clock` still waits for the timeout).
- After the fix: 12 of 12 runs of the failing chunk passed. A deterministic end-to-end reproduction needs engine-owned threads with a clock stepped from another thread; the app-owned runtime re-polls sleeping timers whenever it idles, so it never reaches this state. The regression test is therefore the unit test `a_room_wait_sees_a_stopped_clock_through_its_recheck_timer`, plus the CI repeat runs.
- Not new in the tiered/FIFO work as such. 0.1.0 passed 4 of 4 runs of the same chunk, but this seed's timing reached the state only once the public suite's families changed with #241.
