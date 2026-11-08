# 0123: pigeonhole-memory's single-attempt writes and the consolidation loop's async wait

**Status:** Approved (number from the coordinator, 2026-11-05, for #1479 PR 2's design addendum, agreed by perf287 and cli692; amended the same day for `prepare_remember`, `remember_prepared_in` and `poll_wait`).

## The change
pigeonhole-memory (semver, additive):
- **`Store::link_in`, `Store::supersede_in` and `Store::mark_used_in` (`&mut Transaction`):** the bodies of `link`, `supersede` and `mark_used`, moved out of their `transact` closures. The sync methods wrap them, so each write has one body and no behaviour changes. Each keeps its in-transaction reads: `link_in` reads both ends' `content:text`, so a concurrent delete conflicts at commit. `supersede_in` and `mark_used_in` take the time of the call. The sync forms still take theirs once, before their retries.
- **`Store::prepare_remember(ns, memory) -> PreparedMemory` and `Store::remember_prepared_in(txn, &PreparedMemory)`:** `remember`, step by step.
  - `prepare_remember` checks the memory, runs the near-dedup lookup if the store has near dedup (a read outside any transaction), and fixes the new memory's id and time.
  - `remember_prepared_in` is one attempt of the transaction, so a caller's retry loop writes the same memory every time.
  - `remember` is now `prepare_remember`, then `transact(remember_prepared_in)`, then `settle`, with no behaviour change. Unlike `remember_in`, this keeps near dedup.
- **`Store::settle(&self)`:** what `remember` and `delete` do after their transaction. It hands the IVF index's parked writes to `after_commit`, which splits or merges the lists they left out of bounds. A caller that commits a `remember_in` or `delete_in` transaction itself calls it. Its errors are dropped, as before. Without it, recall is still correct: a list stays over its bound until a later write settles it. (It was the crate-private `ivf_settle`.)
- **`ConsolidateLoop::poll_wait(cx)` and `ConsolidateLoop::wait_async()`:** the loop's wait for new memories (its `run_once`'s read step), as a poll and as a future. They record a commit's events in the same poll that takes it, so dropping the future loses nothing; they run no pass and have no timeout. A loop that hasn't run a step yet, or whose feed history was recycled, returns 0 at once, and the next `run_once` starts it or catches it up (neither scan runs in a poll). The poll form exists for a binding that needs a `'static` future, as `Watch::poll_next` does.
- **The retry policy:** the Python bindings' async writes retry with `RetryPolicy::default()`, the sync methods' policy, through `_Retry`. So `link_async` and `link` behave the same under contention.

## Why
#1479 PR 2 (the owner's tier 1): the Python memory store's awaitable writes loop single attempts with `commit_async`, as `transact_async` does, and its consolidation loop awaits new memories without a thread. That needs a single-attempt form of each write, and a cancel-safe async wait in the loop.

## Callers changed
The sync `link`, `supersede`, `mark_used` and `remember` now go through the new forms. Their behaviour is unchanged, and the memory suite passes. The Python bindings are the first outside caller.
