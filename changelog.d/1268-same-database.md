### Added
- `pigeonhole`: `Pigeonhole::same_database(&other)` says whether two handles come from the same open database (ICR 0110).
- `pigeonhole-vector`: a second `Index::open` of an index already open in the process returns a clone of the open handle, sharing its centroid cache, its rebalancing queue and its one rebalancing thread (#1268, ICR 0100).
