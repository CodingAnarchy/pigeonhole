### Added
- **`Recall::run_async` and `Recall::run_with_stats_async`** in `pigeonhole-memory` (the `async` feature, default-on, #1654, #1560, ICR 0124). A recall as a `Send` future, returning exactly what `run` does. It works in reader processes too.
  - Every read is async: the IVF counters, the vector search (exact or IVF), the keyword search, the recency scan, the candidates' multi-get and the graph expansion's traversals.
  - Each namespace's vector, keyword and recency lists are read together.
  - A row longer than a block in a key-set read still has its later blocks read synchronously (#1652).
- `pigeonhole-vector`'s `Search::run_async_with_stats` can be found in the docs as `run_with_stats_async`.
