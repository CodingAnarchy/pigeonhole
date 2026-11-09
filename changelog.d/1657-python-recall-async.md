### Added
- **`Store.recall_async` and `Store.recall_with_stats_async`** in the Python bindings, and the same on `ReadStore` (#1657, #1560, ICR 0124). They take `recall`'s keywords and resolve to the same result, with the recall's reads asynchronous.
  - A recall is never polled at the call. It starts on a small pool of poll threads (2 to 8), so concurrent recalls hold up neither the event loop nor the commits awaited beside them.
