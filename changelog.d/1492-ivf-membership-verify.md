### Added
- `pigeonhole_vector::Index::verify_at` and `Index::ids_at`: the index's row checks, and its vector ids, at a given snapshot (#1427).

### Changed
- `pigeonhole_memory::Store::verify` checks an IVF store's index by membership, not by count. `StoreStats::ivf_missing` counts memories with an embedding that the index lacks, and `StoreStats::ivf_stale` counts index vectors with no such memory. `ivf_mismatched` is now their sum. Both sides are read at `verify`'s one snapshot, so `is_consistent` now counts `ivf_mismatched`, as do Ruby's `consistent?` and Python's `is_consistent`. Python gains `StoreStats.ivf_mismatched` (#1427).
