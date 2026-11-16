### Added
- `phdb serve`: `Vectors.Nearest` takes `qualifiers` (a `QualifierSelection`: a prefix, bounds, or both) and ranks only the vectors at the qualifiers it selects, on one node and across a cluster's partitions (#1915).
- `phdb serve`: `Data.Batch` takes every write `Mutate` takes: `u64` and vector puts (vectors checked against the family's spec) and aggregate inputs, which it refused before (#1915). On a cluster mid-upgrade, a node still on the older build refuses them with `InvalidArgument`.
