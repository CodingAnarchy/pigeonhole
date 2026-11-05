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
