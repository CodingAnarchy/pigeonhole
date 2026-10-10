# 0028: `reap_orphan_io`

**Status:** Approved (coordinator, 2026-10-10; #473's scaling hang).

## Change

`pigeonhole-io`, additive:

```rust
/// Reaps every backend no thread reaps (application-owned io_uring's shared ring, #408),
/// waiting up to `wait` for something to complete. `None` when no such backend is
/// registered (one relaxed load); otherwise whether anything completed.
pub fn reap_orphan_io(wait: Duration) -> Option<bool>;
```

It is the crate-private `reap_orphans` that `Completion::wait` already uses while it waits (#408), made public.

## Why

`phdb-bench scaling` on `PIGEONHOLE_IO=uring` hung on Linux (#473's dry run). gdb showed a shard thread in `final_sync → Wal::sync → durable_sync`, waiting on the stream's sync ordering (D58) for an older sync that sat on the shared ring.

- Since #408 (#443), that ring has no reaper in application-owned mode. A blocked `Completion::wait` reaps it, but the WAL's own blocking waits (`durable_sync`'s condvar loop, the held-header/rollover wait) reaped only the calling thread's own ring.
- So they waited for ever on I/O only an orphan reap would complete.
- A second cause, fixed beside it with no API change: the shard runtime attached a ring once, on the first thread that ran the shard. A shard moved to another thread (as `phdb-bench`'s inline runner does between phases) then submitted to the shared ring with no ring of its own reaping it. It now attaches per thread.

## Semantics

- **Unchanged reaping:** `reap_orphan_io` reaps exactly what a blocked `Completion::wait` with no drive reaps.
- **Who calls it:** a caller blocked on an outcome chained after I/O it holds no completion of, a WAL sync waiting for older syncs being the one in-tree case. It calls `reap_orphan_io` when it has no I/O of its own in flight, and sleeps as before when it returns `None`.
- **Orphan completions** still run their continuations on the reaping thread, as they do in `Completion::wait`.

## Callers

- `pigeonhole-wal`: `Shared::durable_sync` and the held-header/rollover wait, through one `reap_for_wait` helper: own I/O first, then orphans, then the condvar.
