### Added
- Bloom filter aggregate families (D298, ICR 0096): `Aggregation::Bloom(BloomParams::new(bits, hashes)?)` answers `CellRef::bloom_contains(v)` / `bloom_contains_bytes(b)` with no false negatives, at the configured false-positive rate. Elements are written with `aggregate(i64)` or the new `aggregate_bytes(&[u8])`. Like the other aggregate families, the first one marks the file, which pigeonhole 1.0.x then refuses to open (`UnsupportedFeature`).
- `aggregate_bytes` / `aggregate_bytes_at` also feed HLL families, so string keys need no hashing to `i64` first. Bytes and `i64` elements count apart.
- `phdb`: `"aggregation": "bloom"` with its `aggregation_params`; the shell's `create … name:bloom=m,k`; `aggregate` takes a non-integer word as a bytes element in HLL and bloom families.

### Fixed
- An `f64` put at an explicit timestamp, alone in its commit (`put_f64_at`), into an exact-sum, mean/variance, histogram or quantile family is stored as its one-input state, as at the fixed timestamp. Before, it was stored as a raw `f64`, and those families' readers (`sum_f64`, `mean_var`, `histogram_counts`, `quantile`) returned `None` for it until a later input folded it. Unreleased.
