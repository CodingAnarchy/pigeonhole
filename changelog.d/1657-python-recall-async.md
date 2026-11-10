### Added
- **`Store.recall_async` and `Store.recall_with_stats_async`** in the Python bindings, and the same on `ReadStore` (#1657, #1560, ICR 0124). They take `recall`'s keywords and resolve to the same result.
  - The call returns at once, and the recall runs on a pool of threads the binding keeps for it (2 to 16, by the machine's parallelism; `PIGEONHOLE_ASYNCIO_POOL_THREADS` sets another size), so concurrent recalls hold up neither the event loop nor the commits awaited beside them.
