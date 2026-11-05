### Added
- Python: `Table.scan(...).to_arrow()` returns an `ArrowStream` of the scan's cells as Arrow batches (the `pigeonhole-arrow` schema) for pandas, Polars, DuckDB and pyarrow through the Arrow PyCapsule protocol, with no Python object per cell. pyarrow is optional (`pigeonhole-db[arrow]`, for `to_pyarrow()`) (#1480, ICR 0120).
- `pigeonhole-arrow`: `ArrowScan::from_rows`, the Arrow export of a scan already started (ICR 0120).
