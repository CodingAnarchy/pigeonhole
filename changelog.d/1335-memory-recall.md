### Added
- `pigeonhole-memory` (D311, T1's second PR): hybrid recall.
  - Vector, keyword and recency candidates are fused by reciprocal rank fusion (or a linear blend) within a byte budget, with each memory's signals on request.
  - `Store::open_reader` gives a `ReadStore` for reader processes.
