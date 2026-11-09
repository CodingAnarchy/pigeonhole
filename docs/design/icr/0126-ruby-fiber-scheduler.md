# 0126: Ruby's waiting calls under a fiber scheduler

**Status:** Approved. The coordinator assigned the number (2026-11-05). cli692 reviewed the design note on #1574 and agreed to it as revised (R1–R5, S1–S3, Q1–Q4).

## The change
- **Behaviour (Changed):** under `Fiber.current_scheduler`, the Ruby bindings' commits let the thread's other fibers run while they wait, instead of blocking the thread. The calls are:
  - `WriteBatch#commit` and `Transaction#commit`, and the `write_batch` and `transaction` block forms;
  - `Database#transact`;
  - `Mutation#commit`, `commit_if`, `commit_returning` and `commit_if_returning`;
  - `Table#put`, `incr`, `incr_returning` and `delete`.

  The results and errors are unchanged. Without a scheduler, inside `Fiber.blocking { }`, or on Windows, the calls block as before.
- **New public names:** `Pigeonhole.fiber_scheduler` and `Pigeonhole.fiber_scheduler=` (default `true`). `false` keeps every call blocking, process-wide (cli692's R5).
- **Internal (under `Pigeonhole::Native`, not public API):**
  - `FiberBridge`, in Ruby;
  - its native half, `FiberBridgeCore`;
  - `FiberOp`;
  - the private `native_*_op` methods on `Mutation`, `WriteBatch` and `Transaction`, and `Pigeonhole.native_fiber_bridge`.

  `Mutation#commit` and `commit_if` are now Ruby methods over private natives, with the same names and signatures.
- **A dependency:** `nix` 0.29 (`fs`, Unix only) for `fcntl`, to make the pipe non-blocking.

## How
This is the design note on #1574, §3:
- **The waker:** the engine's waker queues the op and writes one byte to the thread's pipe, at most once per drain. It takes no GVL.
- **The drainer:** one parked fiber holds the drainer role. It waits on the pipe through the scheduler, polls every woken op in one call without the GVL, and resumes the fibers whose ops are done. The role passes on in its holder's `ensure`.
- **Resumes:** `block` gets no timeout, and every return from it is re-checked.

## Amendment 1: the feed and the memory watch (#1574 PR 2)
- **Behaviour (Changed):** under a scheduler, `Feed#next` and `each`, and memory `Watch#next` and `each`, let the thread's other fibers run while they wait. `next(timeout:)` returns nil once the timeout passes, as before.
- **Fiber-wait rules:**
  - **Readers:** a reader process's watch raises `Unsupported` for a fiber wait, because the engine's poll is the writer's only. A thread can still wait on it.
  - **One consumer at a time:** a second fiber's `next` raises `Pigeonhole::Error` while one waits, and so does a thread's `next` while a fiber waits, or the reverse.
  - **Stops lose nothing:** a stopped fiber's commit stays in a hand-back slot, and `token` gives the position from before it.
- **The core:** `crates/ruby/src/waits.rs`, a port of the Python bindings' `waits` core (#1479), now holds both streams' state. A sync read takes the source out, other sync callers take turns (#1411's fairness kept), and a generation counter retires a dropped wait.
- **Deadlines** live on the bridge (cli692's R2): the drainer waits on the pipe up to the nearest deadline and resumes expired waiters itself. A stopped or timed-out fiber's op is dropped at once, a pending future or an unclaimed result alike.

## Cancellation
A fiber raised into while it waits drops its op's future, as D210 defines:
- a commit still waiting for room writes nothing;
- one already submitted may land, and its result is lost.

So it's the same outcome as Python's asyncio forms (ICR 0121).

**A raise crossing a resume already sent** (the design note's R2 residual, cli692's N1): the drainer has unblocked a fiber whose op is done, and before that resume runs, a raise resumes the fiber instead. The scheduler still holds the unblock. The fiber's `ensure` first drops the op's future, so no drain can submit a commit still waiting for room during the wait that follows. It then takes the resume with one more `block`, bounded at 1 s and with interrupts masked, before it unwinds. So the stale resume can't end the fiber's next, unrelated wait early. This relies on the scheduler keeping an unblock for a fiber that wasn't blocked when it arrived, as queue-based schedulers do (Async, and Ruby's test scheduler). A scheduler that drops it costs the stopping fiber up to 1 s, never a stray resume. CI's minimal-scheduler run found the case before the absorb was added.

## Known effects
- **After a fork,** the child leaves the parent's bridge open (two file descriptors for each thread that had waited) and makes its own (S1).
- **Memory recall and the consolidation loop's step** follow in #1574's later PRs, and they're named here when they land.
