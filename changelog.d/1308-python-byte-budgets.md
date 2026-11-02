### Added
- Python (`pigeonhole-db`): D308's byte-budgeted reads.
  - `Table.scan_within`, `Table.multi_get_within` and `Table.nearest_within` return pages that fit a byte budget.
  - `ScanPosition` and `KeysPosition` resume a scan or multi-get with `after=`.
  - `traverse(byte_budget=...)` bounds a traversal, and reports `cut_by_budget`.
