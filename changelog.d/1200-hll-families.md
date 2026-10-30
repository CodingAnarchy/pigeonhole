### Added
- HyperLogLog aggregate families (D294, ICR 0094): `Family::aggregate(Aggregation::Hll)` counts the distinct `i64` inputs of each cell, written with `aggregate` / `aggregate_at` and read with `CellRef::hll_estimate` (4096 registers, about 1.6% standard error). A cell is a sketch of up to 3 KiB; reading a hot one takes about 14 µs at least. `incr` and `put_i64` are refused there, and `put` takes only a well-formed sketch (as `phdb load` writes one back).
  - Like min and max families, creating the first HLL family marks the file, which pigeonhole 1.0.x then refuses to open (`UnsupportedFeature`).
- `phdb`: `"aggregation": "hll"` and the shell's `create … name:hll`.
