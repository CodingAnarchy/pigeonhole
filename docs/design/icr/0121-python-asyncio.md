# 0121: native asyncio in the Python bindings

**Status:** Approved (number from the coordinator, 2026-11-05). The design is #1479's note (issuecomment-6654648701), agreed by cli692 (the bindings) and perf2 (the async front door).

## The change
The Python bindings (ICR 0102's stubs) gain the awaitable forms of the calls that wait.
- **The new names:**
  - `Mutation.commit_async()`, `Mutation.commit_if_async(condition)` and `Mutation.commit_returning_async()`;
  - `Table.incr_returning_async(row, family, qualifier, delta, *, ts=None, durability=None)`;
  - `WriteBatch.commit_async(*, durability=None)` and `Transaction.commit_async(*, durability=None)`;
  - `Transaction.get_async(table, row, family, qualifier)`;
  - `Feed.next_async()`;
  - `Database.transact_async(f, *, max_attempts=None, deadline=None, durability=None)`.
- **What they return:** each but `transact_async` returns an `asyncio.Future` of the running loop, typed `asyncio.Future[T]` with the sync form's `T`. `transact_async` returns a coroutine.
- **The private names,** not in the stubs:
  - `_native._Retry`, `transact_async`'s handle on `RetryPolicy::next_pause`;
  - `_native._Gate`, a hold seam for the bridge's tests, built only with the `test-hooks` feature, never in the wheels.
- **Behaviour changes to existing names:**
  - `Database.transact(f)` now refuses an `async def` body with `TypeError`. Before, it committed an empty transaction and returned an unawaited coroutine.
  - `Feed.next`, `try_next` and `token` honour the hand-back slot: a commit `next_async` took after its future was cancelled comes first, and `token()` points before it.
  - While a `next_async` wait is in progress, `next` and `try_next` raise `ValueError`.
- **Unchanged:** no Rust crate's API. `pigeonhole` already has the futures (D196, D210) and `RetryPolicy::next_pause`.

## Why
#1479: agent frameworks are async, and they call the sync API through `asyncio.to_thread`, at a thread per in-flight call. The engine's async front door already resolves commits without a thread.

## Callers changed
None outside the bindings. The memory store's async forms are #1479's second PR, which needs single-attempt additions to pigeonhole-memory and the owner's scope call.
