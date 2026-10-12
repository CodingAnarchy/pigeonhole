# 0035: an application-owned shard's own wait (`Shard::wait`, `Shard::waker`)

**Status:** Approved (owner, via the coordinator, 2026-10-12; #553).

## Change

`pigeonhole-runtime`, additive:

```rust
impl<H: ShardHandler> ShardDriver<H> {
    /// Waits until this shard has work again: a message arrives, a `waker()` fires, the
    /// calling thread's own I/O completes, the next background deadline passes, or `timeout`.
    pub fn wait(&mut self, timeout: Duration);
    /// A waker that ends this shard's `wait` from any thread.
    pub fn waker(&self) -> std::task::Waker;
}
```

`pigeonhole-engine` (`EngineShard`) and `pigeonhole` (`Shard`), additive, the same two methods. `Shard::wait` becomes the documented loop for one shard per thread. `next_wakeup`, `io_fd` and `set_wakeup` stay, for applications that wait in their own event loop.

## Why

#553: in application-owned mode with io_uring (the default on Linux since D218), a shard's I/O completes only when its thread reaps its own ring. `Shard::next_wakeup` therefore reports in-flight I/O as due now unless the application takes `Shard::io_fd` and waits on it. A loop that sleeps for `next_wakeup` without taking the fd busy-polls. The phase 3 gate's bench driver did, which inflated the D213 group-commit p99 from about 300 µs to 1.3–3.5 ms. The instruction-count examples did the same as soon as D218 made io_uring the default. The documented contract was right, but its default path was a trap.

`wait` gives one call that does the right thing on every backend, with no file descriptor and no event loop.

## Semantics

- **The handshake is protocol 1's** (`concurrency.md`). `ShardDriver::wait` runs `Signal::sleep_unless`: announce sleep (`prepare_sleep`), re-check the inbox and `task_woken`, and sleep only if neither has anything.
- **The sleep:** it waits in the thread's ring (`reap_own_io(Some(t))`) while the thread's own I/O is in flight, and parks (`thread::park_timeout(t)`) otherwise. `t` is the smaller of `timeout` and the time to the next background deadline.
- **The wake target:** the first `wait` on a thread installs the waiting thread as the shard's wake target. Without a `set_wakeup` callback, that is the engine-owned thread's own target: `WakeTarget::Thread`, or `ThreadRing` when the thread has a ring (#556 makes the ring wake an eventfd write only while the thread waits in the ring). Using that target keeps a closure call off every commit's wake (#572). With a callback, it is a composite `WakeTarget::Callback`: the callback, then the ring waker (if any), then an unpark. `set_wakeup` keeps working, and its callback still fires.
- **`waker()`** calls `Signal::notify_task`. That sets `task_woken`, which the handshake re-checks, then notifies. It is for futures the driving thread polls itself: a commit's future can be resolved on another shard's thread, and that wake has to end this thread's wait. A wake while nobody waits only makes the next `wait` return at once.
- **One shard per thread.** `wait` covers its own shard. A thread that drives several shards waits in its own loop, on the wakeup callbacks and the completion fd.
- **No new shared primitive.** The target is `Signal::target` (its existing mutex). The callback and the waiting thread live in the `ShardDriver`, which only its owner touches.

## Callers

- `pigeonhole`: `Shard::wait` and `Shard::waker` call `EngineShard`'s, which call `ShardDriver`'s.
- `pigeonhole-bench`: the inline driver (`runners/inline.rs`) and the instruction-count examples' driver (`examples/support/shards.rs`) switch to `wait` and `waker`. The inline driver drops its own eventfd poll, and the bench crate goes back to `forbid(unsafe_code)`.
- Docs: `docs/guide/async.md`, `docs/guide/agent-reference.md`, and the `Shard` rustdoc.
