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

## Amendment 2: the memory store (#1574 PR 3)
- **Behaviour (Changed):** under a scheduler, these let the thread's other fibers run while they wait:
  - memory `Store#remember`, `delete`, `link`, `supersede` and `mark_used`;
  - `recall` and `recall_with_stats` (on `Store` and `ReadStore`);
  - `ConsolidateLoop#run_once`.

  The results and errors are unchanged. `Store#delete` and `supersede` are now Ruby methods over private natives, with the same names and signatures.
- **Writes** run as the crate's own do, step by step (ICR 0123): the store's `*_in` form inside `Database#transact`, whose commit yields through the bridge (retried by the same `RetryPolicy`), then `settle` for an IVF store. `remember` first runs `prepare_remember`.
- **The worker-thread jobs** (§3.5 of the design note): `recall`, `remember`'s near-dedup lookup (when the store has near dedup) and `settle` run on `pigeonhole-ruby-offload`, a pool of the binding's own of at most `min(available_parallelism, 8)` threads, started as jobs need them, and complete through a fiber op. A job not yet started when its op is dropped is skipped; once started, it runs to its end and its result is dropped. A forked child starts a fresh pool.
- **`ConsolidateLoop#run_once`:**
  - **The wait** goes through the bridge, one commit at a time with `ConsolidateLoop::poll_wait`, until the timeout or the next pass falls due, as the blocking step's read does. The wait holds the loop meanwhile, and `close` wakes it, so it raises at once (#1606's lesson).
  - **The step** then runs `run_once(0)` on its worker, as before, but the fiber waits for the consolidator's groups through the bridge, and calls the block on itself, so the block may yield.
  - **A stop during the passes** is recorded as an interrupt is: later groups are refused, the fiber keeps waiting through the bridge until the worker ends, and then the stop is raised. So no thread outlives the call, and the loop is ready for the next step. (This replaces the design note's `handle_interrupt(:never)`, which doesn't mask a scheduler's `Fiber#raise`.)
  - **A loop's first step** starts it without waiting, because a loop that hasn't stepped has no position to wait from.
- **New internals** (under `Pigeonhole::Native`, not public API): `PreparedMemory`; the private natives `native_prepare_remember`, `native_prepare_remember_op`, `native_remember_prepared_in`, `native_link_in`, `native_supersede_in`, `native_mark_used_in`, `native_near_dedup_on`, `native_has_ivf`, `native_settle_op` and `native_recall_op` on the stores; and `native_wait_op` and `native_run_once_fiber` on `ConsolidateLoop`. With `test-hooks`, `Native.offload_hold` holds the pool's jobs, a hold seam for the tests.

## Amendment 3: recall stays on the offload pool (#1574 PR 4, measured)
- **The plan was** to move `recall` and `recall_with_stats` off the offload pool once `Recall::run_async` (#1560 PR 3, #1654) landed, polling it through the bridge on the fiber's own thread as the commits are.
- **Measured,** the move loses throughput. Under Async on a Mac: 2,000 memories, recall with text and an 8-d embedding at k 10, 40 recalls per fiber, and a fiber timing `durability: :sync` puts alongside. Two runs each:

  | build | c=1 recalls/s | c=16 recalls/s | commit p50 alongside (c=16) |
  |---|---|---|---|
  | offload pool, sync recall (kept) | 199 / 205 | 2,952 / 3,215 | 4.96 / 3.71 ms |
  | `Recall::run_async` polled on the scheduler's thread | 160 / 154 | 866 / 873 | 3.75 / 3.75 ms |

  - **Why:** the pool runs up to 8 recalls in parallel on other cores, while polling on the scheduler's thread runs every recall's CPU on that one thread. At c=1 the async recall itself costs about 1.3× the sync one (perf2's measurement on #1657).
  - **Commits** alongside are no worse on the pool: the sync commit's fsync dominates.
  - **Python found the same** with one bridge thread (#1657: 66 recalls/s against `to_thread`'s 315), and added a poll pool there.
- **So `recall` stays on the offload pool.** #1574's goal, that a recall doesn't block the fiber scheduler, is already met that way, at about 3.6× the throughput. Nothing changes in the binding.
- **What the pool path still costs:** a recall stopped while it runs finishes on its worker, and its result is dropped (the Cancellation section's memory line).
- **Revisit** if the async recall's per-call cost falls below the sync one's (#1652, and perf287's box measurements of the ratio). A pool of poll threads driving `run_async`, like #1657's, would then be the shape to measure against this one.

## Cancellation
A fiber raised into while it waits drops its op's future, as D210 defines:
- a commit still waiting for room writes nothing;
- one already submitted may land, and its result is lost.

So it's the same outcome as Python's asyncio forms (ICR 0121).

**A raise crossing a resume already sent** (the design note's R2 residual, cli692's N1): the drainer has unblocked a fiber whose op is done, and before that resume runs, a raise resumes the fiber instead. The scheduler still holds the unblock. The fiber's `ensure` first drops the op's future, so no drain can submit a commit still waiting for room during the wait that follows. It then takes the resume with one more `block`, bounded at 1 s and with interrupts masked, before it unwinds. So the stale resume can't end the fiber's next, unrelated wait early. This relies on the scheduler keeping an unblock for a fiber that wasn't blocked when it arrived, as queue-based schedulers do (Async, and Ruby's test scheduler). A scheduler that drops it costs the stopping fiber up to 1 s, never a stray resume. CI's minimal-scheduler run found the case before the absorb was added.

## Known effects
- **After a fork,** the child leaves the parent's bridge open (two file descriptors for each thread that had waited) and makes its own (S1).
- **`recall`** stays on the offload pool: measured against `Recall::run_async` polled on the scheduler's thread, the pool kept about 3.6× the throughput at 16 concurrent recalls (amendment 3).
