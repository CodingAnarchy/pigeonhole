### Added
- Python: native `asyncio` forms of the calls that wait (#1479, ICR 0121):
  - `commit_async`, `commit_if_async` and `commit_returning_async` on mutations;
  - `Table.incr_returning_async`;
  - `commit_async` on write batches and transactions;
  - `Transaction.get_async`;
  - `Feed.next_async`;
  - `Database.transact_async` for an `async def` body.
  - They return `asyncio.Future`s, and an awaiting coroutine holds no thread: one bridge thread hands results to the event loops, and the engine's threads never take the GIL.

### Changed
- Python: `Database.transact` refuses an `async def` body with `TypeError` (use `transact_async`).
- Python: `Database.transact`'s retries now count in the engine's `transaction_retries`, as the library's `transact` counts its own; `transact_async`'s count too.
