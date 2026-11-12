### Added
- Python: the memory store under asyncio (#1479, ICR 0121 amendment 2):
  - `Store.remember_async`, `delete_async`, `link_async`, `supersede_async` and `mark_used_async`;
  - `Store.watch` and `ReadStore.watch`, which give a `Watch` of memory events (`next`, `try_next`, `next_async`). Closing a feed or watch ends a pending `next_async` with `Closed`;
  - `Store.consolidate_loop`, which gives a `ConsolidateLoop` (`run_once`, `run_once_async`);
  - `Store.link_in`, `supersede_in`, `mark_used_in` and `settle`.
- `pigeonhole-memory`: `Store::link_in`, `supersede_in` and `mark_used_in`; `prepare_remember` and `remember_prepared_in` (`remember` step by step); `settle`; and `ConsolidateLoop::wait_async` and `poll_wait`, a cancel-safe wait for new memories (ICR 0123).
