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

## Cancellation
A fiber raised into while it waits drops its op's future, as D210 defines:
- a commit still waiting for room writes nothing;
- one already submitted may land, and its result is lost.

So it's the same outcome as Python's asyncio forms (ICR 0121).

## Known effects
- **After a fork,** the child leaves the parent's bridge open (two file descriptors for each thread that had waited) and makes its own (S1).
- **The change feed's, memory's and recall's waits** follow in #1574's later PRs, and they're named here when they land.
