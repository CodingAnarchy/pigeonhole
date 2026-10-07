# Engine open questions

## Decision (coordinator, 2026-10-07): amended D88, blocking on a shard-driving thread (issue #135)
D88 warned only against `PendingCommit::wait` on the thread that drives *the commit's* shard. Visibility is the minimum over every shard's watermark, so a wait can need *any* shard this thread drives (an unresolved WAL group there holds the global watermark), and a commit can be rerouted to another shard. Supporting the wait in every case would need the waiting thread to run its own shards, which it cannot do without the `EngineShard`.

**Decided (coordinator): never report `InvalidArgument` for a write that still lands.**
- The engine records which thread last ran each application-owned shard (`EngineShard::run_once`; cleared when the shard is dropped).
- (a) Blocking calls that submit *and* wait check that condition **before submitting**. On a driving thread, `Engine::commit`, `check_and_mutate`, `Txn::commit`, `flush` and `compact` fail with `InvalidArgument`, and nothing was submitted. Catalog changes (`create_table`, `add_family`, `drop_table`) and `shrink` are not refused: they wait only for the manifest writer. A blocked thread finishes a pump commit whose root commit completed (`manifest::Flight`), so they cannot deadlock on a driving thread.
- (b) `PendingCommit::wait()` on a driving thread, for a commit already submitted, returns the result if it is already done and visible. Otherwise it returns the new `WouldDeadlock` (engine `Error::WouldDeadlock`, public `ErrorCode::WouldDeadlock` = 26). Its message says the commit was submitted and will apply, but its outcome cannot be awaited on a thread that drives a shard: poll the future from the event loop.
- Off driving threads, and always in engine-owned mode, the waits park on the shards' publishes instead of spinning. A visibility wait also ends, with `Closed`, once the close has finished or a shard died (its thread panicked, or its `EngineShard` was dropped). Both events wake every waiter.
- Known gap: a thread that holds an `EngineShard` it has never run is not detected (documented in the rustdoc and agent reference: run each shard before committing from its thread). Another known gap: a thread that handed its shard to another thread counts as the driver until the new thread first runs it.

## Proposed decision: the application-owned close reports its outcome (amends D88; issue #135)
D88 says `close` returns once shutdown is requested. The documented loop ("drive until `run_once` returns `false`, then drop") abandoned the close whenever its flush/checkpoint I/O was in flight, so the close was unclean, and `close` returned `Ok` even when the final sync failed.

**Interim behavior:** `EngineShard::closed()` / `Shard::closed()` (new, additive) returns `Some(outcome)` once the whole close has finished; the documented loop keeps driving each shard (parking on the wakeup as usual: `run_once` still returns `false` while I/O is in flight) until it is `Some`, then drops it. `close()` called on a thread that drives no shard, once every shard has been run at least once (or dropped), waits for the outcome and returns it. On a driving thread, or while some shard has never been run, it returns `Ok(())` after starting the close, and that `Ok` says nothing about the outcome, which is then **only** in `closed()` (stated in `close`'s rustdoc). `Drop for Engine` never waits in application-owned mode.

## Proposed decision: a panicked engine-owned shard thread fails the close (issue #135)
**Interim behavior:** a shard thread that unwinds reports its shard closed (close marked unclean, no final sync) from `ShardState`'s drop, so `close` and `Drop for Engine` end. `close` then returns `Io("shard thread panicked: …")` instead of resuming the panic.
