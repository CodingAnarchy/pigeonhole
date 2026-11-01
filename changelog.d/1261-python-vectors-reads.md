### Added
- Python bindings: vector families (`Family.vector`, `put_vector` from NumPy arrays, other buffers or sequences, `Cell.as_vector` as an `array.array`), `Table.multi_get`, `Table.traverse` and `Table.pages` with resumable `RowPosition`s, and `ScanIter.close()` (#1261). There is a per-operation overhead benchmark, and a Python guide page whose examples run as tests.
