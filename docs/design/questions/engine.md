# Engine questions

## Q: How does a write stall end when the clock moves only with the caller (issue #70)?
D119 cancels the L0 stall's timer when a compaction commits and D124 times a wait for arena room out after `write_stall_timeout_nanos`, but both lean on a clock. Under `SimVfs` the clock moves only when the workload advances it, and the workload is blocked in the commit: the shard loop never reaches its slice deadline, so a `StallTimer` polling the clock spins for ever, and a failed background compaction with an L0 score `>= 1.0` was retried at once by `maintain`, failing in a loop against a dead (crashed) device. A stall that engaged with no compaction running had nothing to end it either.

**Interim behavior:**
- A stall never busy-spins: a `StallTimer` that sees the clock unchanged for 1024 polls in a row gives up (it is only a shortcut for a running clock); the shard arms a new one at the next event that finds it still stalled.
- The L0 stall engages only while a compaction is running (the stall starts one if none runs; when none can start, writers are admitted). Every compaction completion, success or failure, kicks a waiting group, and a successful one adds a token to the bucket: on a frozen clock the stall is paced by compaction progress instead of time.
- After a failed background compaction, `maintain` starts none until a flush or an admitted group clears the backoff (the old rule retried at once while the score was `>= 1.0`): each retry is tied to the caller's progress, so a dead device does not loop, and a stalled writer is admitted rather than held.
- A wait for arena room also ends with `Busy` after 4 flushes in a row failed (a frozen clock never reaches the timeout); a poisoned pager still fails it with the poison error at once.
