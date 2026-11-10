"""`Database.transact_async` (#1479, ICR 0121): `transact` for an `async def` body.

Each attempt begins a transaction, runs `await f(txn)`, then `await txn.commit_async()`. A
`Conflict` or `Busy` is retried as the sync `transact` retries it: `_Retry` asks the library's
`RetryPolicy` for each pause, so the attempts, the backoff and the deadline are the same, and
the event loop does the sleeping (`asyncio.sleep`).
"""

import asyncio
import inspect

from pigeonhole import _native  # type: ignore[attr-defined]  # the compiled module


async def transact(db, f, max_attempts, deadline, durability):
    if not callable(f):
        raise TypeError("transact_async takes an async def body")
    retry = _native._Retry(db, max_attempts, deadline)
    while True:
        txn = db.transaction()
        try:
            body = f(txn)
            if not inspect.isawaitable(body):
                raise TypeError(
                    "transact_async takes an async def body; use transact for a sync one"
                )
            out = await body
            await txn.commit_async(durability=durability)
            return out
        except (_native.Conflict, _native.Busy) as e:
            txn.rollback()
            pause = retry.next_pause(e)
            if pause is None:
                raise
            await asyncio.sleep(pause)
        except BaseException:
            txn.rollback()
            raise


# ---- the memory store (#1479 PR 2, ICR 0121 amendment 2, ICR 0123) ----


async def _attempts(store, attempt, settle):
    """Single attempts of one memory write, retried as `transact` retries (`_Retry`), each
    committed with `commit_async`. `attempt(txn)` runs the store's `*_in` form: its reads in
    the transaction may block the loop on a cache miss. With `settle`, an IVF store's parked
    writes are handed on afterwards, on a worker thread (its splits commit)."""
    db = store._db
    retry = _native._Retry(db)
    while True:
        txn = db.transaction()
        try:
            out = attempt(txn)
            await txn.commit_async()
            break
        except (_native.Conflict, _native.Busy) as e:
            txn.rollback()
            pause = retry.next_pause(e)
            if pause is None:
                raise
            await asyncio.sleep(pause)
        except BaseException:
            txn.rollback()
            raise
    if settle and store._has_ivf():
        await asyncio.to_thread(store.settle)
    return out


async def memory_remember(store, ns, text, kwargs):
    # The near-dedup lookup reads outside the transaction (a vector search): off the loop.
    if store._near_dedup_on():
        prepared = await asyncio.to_thread(store._prepare_remember, ns, text, **kwargs)
    else:
        prepared = store._prepare_remember(ns, text, **kwargs)
    return await _attempts(store, lambda txn: store._remember_prepared_in(txn, prepared), True)


async def memory_delete(store, id):
    return await _attempts(store, lambda txn: store.delete_in(txn, id), True)


async def memory_link(store, from_, to, link_type, weight):
    return await _attempts(
        store, lambda txn: store.link_in(txn, from_, to, link_type, weight), False
    )


async def memory_supersede(store, old, new):
    return await _attempts(store, lambda txn: store.supersede_in(txn, old, new), False)


async def memory_mark_used(store, ids):
    return await _attempts(store, lambda txn: store.mark_used_in(txn, ids), False)


async def memory_loop_step(loop, timeout):
    """`ConsolidateLoop.run_once_async`: the wait for new memories, raced against the next
    pass's due time, holds no thread (a cancelled wait loses nothing: the loop records a
    commit in the poll that takes it); the passes, which call the consolidator, run on a
    worker thread."""
    wait = float(timeout)
    due = loop.next_due
    if due is not None:
        wait = min(wait, max(0.0, (due - loop._now_us()) / 1e6))
    waited = 0
    if wait > 0:
        try:
            waited = await asyncio.wait_for(loop._wait_async(), wait)
        except TimeoutError:
            pass
    return await asyncio.to_thread(loop._run_once_after, waited)
