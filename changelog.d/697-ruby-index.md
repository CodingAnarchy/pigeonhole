### Added
- Ruby bindings, PR 3b (#697, ICR 0111): full-text search and the vector index.
  - **`Pigeonhole::Text`:** `Index` and `ReadIndex` over `pigeonhole-text`: put and delete, in a transaction too; search by query string or built `Query`, with phrases, fuzzy terms, highlights (byte ranges), id prefixes and stats; verify, rebuild and repair.
  - **`Pigeonhole::Vector`:** `Index` and `ReadIndex` over `pigeonhole-vector`: insert and delete, in a transaction too, with `Placed` and `after_commit`; search with nprobe, id prefixes and stats; a bulk load that streams its pairs in batches; and a `Rebalancer` a Ruby thread drives.
  - **`Pigeonhole::Memory` gains T8's IVF recall:** `Store.create(ivf: IvfOptions.new(...))`, `Store#ivf`, `recall(exact:, nprobe:)`, and the IVF counts on `RecallStats` and `StoreStats`.
