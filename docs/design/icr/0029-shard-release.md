# 0029: `Shard::release` / `EngineShard::release`

**Status:** Approved (coordinator, 2026-10-10; #492).

## Change

`pigeonhole`, additive:

```rust
impl Shard {
    /// Call before handing this shard to another thread: drives it until the calling
    /// thread has none of its I/O in flight. A no-op on the pread backend and in
    /// engine-owned mode.
    pub fn release(&mut self);
}
```

`pigeonhole-engine` (`EngineShard::release`) and `pigeonhole-runtime` (`ShardDriver::release`), the same, beneath it.

## Why

With io_uring in application-owned mode, each thread that drives a shard has its own single-issuer ring, and only that thread can complete what was submitted to it (#402, #408). #485 makes a shard attach a ring on every thread that runs it, so its I/O *after* a move goes to the new thread's ring. But I/O already on the old thread's ring completes only when that thread runs another turn, or when it exits (a thread ring drains when its thread ends). If the old thread stays alive and stops running turns, the shard waits for ever: its next blocking WAL sync waits for the stranded sync (D58). Only the owner may reap a single-issuer ring, so no other thread can help. The application has to drain before moving, and this is the call for it.

## Semantics

- **On the shard's driving thread with per-thread rings:** runs the shard's turns and reaps the thread's own ring until `own_io_in_flight()` is false. The completions' continuations run there and queue their results on the shard (`SyncDone` and the like), and the next thread to run the shard handles them.
  - It also drains other shards' I/O on the same thread's ring. That's harmless: the same thread drives them.
- **Otherwise it returns at once:** the pread and synchronous backends have no per-thread rings, engine-owned shards never move, and a thread that hasn't run this shard has nothing of its own to drain.
- **The rule (docs):** call `release()` on the old thread before moving a shard to another thread, then take `io_fd` again on the new thread.
- **Debug builds enforce it.** The runtime records whether the thread's ring still had I/O in flight at the end of each turn. A shard that next runs on another thread with that flag set and no `release()` in between panics: "moved to another thread while its I/O was in flight on the old thread's ring; call release() on the old thread first (#492)". Release builds carry none of this: the instruction shapes measure them.

## Callers

- `phdb-bench`'s inline runner (`phase`, `with_drivers`) releases before handing shards back.
- `pigeonhole/tests/app_owned_uring.rs`'s moved-shard test releases in `drive_until`.

## Tests

`pigeonhole/tests/app_owned_uring.rs`:

- **`release_drains_a_sync_in_flight_on_the_old_threads_ring_before_the_shard_moves`:**
  - a durable commit's sync is in flight on the old thread's ring when the shard moves;
  - the old thread stays alive and idle;
  - the commit must resolve on the new thread.

  With `release()` mutated to return at once, it fails in both builds: the commit times out in release builds, and the detector panics in debug builds.
- **`debug_builds_reject_a_shard_moved_with_its_io_left_on_the_old_thread`:** the detector.
- **`a_released_shard_moves_off_a_thread_that_stays_alive_and_its_io_completes`:** the ordinary move, released, with the old thread alive.
