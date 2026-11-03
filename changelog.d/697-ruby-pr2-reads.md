### Added
- Ruby bindings, PR 2 (#697, ICR 0111): vectors and the 1.1 reads.
  - **Vectors:** vector families (`Family.vector`, `VectorSpec`), `put_vector` from Arrays (or packed bits), `Cell#as_vector`, and `Table#nearest`.
  - **Reads:** `Table#multi_get` (aligned with the keys given), `traverse`, and `pages` with resumable `RowPosition`s.
  - **Byte budgets:** `scan_within`, `multi_get_within` and `nearest_within`, with resumable `ScanPosition`s and `KeysPosition`s, and `traverse(byte_budget:)`.
