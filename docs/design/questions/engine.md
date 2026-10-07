# Engine open questions

## Proposed decision: amend D124 and D126 — stalls, retries and backoffs on a moving clock (issue #141)
The #90 review (1-2 F3–F5, 5-6 5.2, 5.3, 5.7) found that several of D124's and D126's event-tied retries, added for the simulator's frozen clock, misbehave on a real clock.

**Interim behavior:**
- *Waits for room re-check (amends D124).* On a moving clock, a wait for arena room and a starved freeze's wait each arm a re-check timer that kicks the shard after 1 ms, doubling up to 100 ms. Room freed by a snapshot dropped on another thread, or by a reader process's unpin, announces itself to nobody, and is now seen within about 100 ms instead of at the 30 s timeout. A frozen clock is unchanged (D126's idle case).
- *Failed flushes back off (amends D124 "a failed flush is tried again on the next retry").* After a failed flush, no flush starts until a backoff timer fires (`RetryFlush`): 10 ms, doubling up to 1 s, reset by a flush that succeeds. A close flushes at once. On a frozen clock the timer gives up and the next trigger retries; `ROOM_FLUSH_ATTEMPTS` still ends a room wait there. Before this, a flush failing during a room wait was retried back to back until the stall timeout (about 17k attempts a second in the test).
- *Compaction backoff is per slot, and on a moving clock only its timer ends it (amends D126 "Failed compactions").*
  - A failed (or unstartable) compaction backs off its `(tablet, family)` slot: 1 s, doubling up to 60 s per slot, reset when the slot compacts. Other slots keep compacting, so one corrupt slot no longer stops compaction for the whole shard. One `RetryCompaction` timer fires at the earliest end of a slot's backoff. Cleanups skip slots that are backing off.
  - A flush completion or an admitted group ends a backoff only on a frozen clock (the timer gave up). On a moving clock, under steady writes, those events restarted a compaction that kept failing after every flush (3035 failed attempts in 2.5 s in the test).
  - `compaction_backoff` remains the stall's "a compaction just failed" signal (D119), and is cleared by the retry timer, a successful compaction, or a frozen-clock event.
  - A dead device costs at most one attempt per due slot per backoff interval.
- *No stale compaction error.* Only a compaction started for a `compact()` caller reports its failure, to that caller, at once. A background failure is nobody's: it backs off. Before, it was kept and returned by a later, successful `compact()`, or handed to a queued caller.

## Proposed decision: a batch that can never fit gets `BatchTooLarge`, not `Busy` (issue #141; refines D16 and D124)
D124 ended a wait for room "with `Busy` ... at once for a batch that can never fit an empty arena". `Busy` therefore meant both "retry" and "never retry", and the arena accounting charged twice every entry's size. The real cap was about half what was documented, so a value of exactly half the arena, which D16 allows, got `Busy`.

**Interim behavior:**
- *Arena accounting.* A memtable loses at most the tail of its current run when an entry does not fit it, and that tail is shorter than both the entry and a chunk. So an entry is charged its size plus `min(size, chunk)`, not twice its size. A single value up to half the arena (D16's limit) always fits. A batch of small entries is still capped at about half the arena: their waste bound is unchanged.
- *New error.* A batch that can never fit an empty arena fails at once with the new non-retryable `Error::BatchTooLarge` (public `ErrorCode::BatchTooLarge = 27`, documented in `errors.md`), also through a cross-shard PREPARE (`PrepareError::NeverFits`). It is not counted as a stall.
- `Busy` now means only "stalled past the write-stall timeout (30 s); retry". The "30 s by default" wording in the public docs is now "30 s": there is no public knob.

## Q: should the public API expose the write-stall timeout? (issue #141)
The engine has `EngineOptions::write_stall_timeout_nanos`, but `pigeonhole::Options` does not expose it. The docs used to say "30 s by default", implying a knob.

**Interim behavior:** the public docs say "30 s". No knob is added.
