# 0120: `ArrowScan::from_rows`, and Python's `scan(...).to_arrow()`

**Status:** Approved (number from the coordinator, 2026-11-05, with the design on #1480: Q1 a protocol object with `to_pyarrow()`, Q3 only on an unread scan).

## The change
- **pigeonhole-arrow** (semver, additive): `ArrowScan::from_rows(rows: RowIter<'t>, options: &ArrowOptions) -> ArrowScan<'t>`, the cells of a scan the caller already started, from the iterator's next row, at the snapshot it took. `scan_to_arrow` is now `from_rows(scan.iter()?, options)`, so its behaviour is unchanged.
- **The Python bindings** (ICR 0102's stubs):
  - **`ScanIter.to_arrow(*, batch_cells=8192, batch_bytes=8388608, row_dictionary=False, utf8_keys=False, typed_values=False, ts="timestamp", vector_dim=None) -> ArrowStream`.** It's `ArrowOptions` one to one. It's refused with `ValueError` on a scan already iterated or closed. It takes the scan, so the iterator then ends.
  - **`ArrowStream`,** a new class:
    - `__arrow_c_stream__(requested_schema=None)` returns the Arrow C stream as an `"arrow_array_stream"` PyCapsule. It works once; `requested_schema` is ignored, as the protocol allows.
    - `__arrow_c_schema__()` returns an `"arrow_schema"` capsule.
    - `schema_names`.
    - `to_pyarrow()` returns `pyarrow.RecordBatchReader.from_stream(self)`; its `ImportError` names the extra.
    - `close()` and the context manager.
  - **The `arrow` extra** is `pyarrow>=18`, the oldest tested. The default install gains no dependency.
  - **The crate** depends on `pigeonhole-arrow` with `ffi`. CONTRIBUTING's layers put `arrow` below `cli` and `python`, which both build on it.

## Why
#1480: analytics users read whole tables into dataframes, and a Python object per cell is the slow path. `Table.scan` starts its scan eagerly (its start errors come out at `scan()`), and the iterator holds a `RowIter`, not a `Scan`, so the bindings can't call `scan_to_arrow`. `from_rows` lets them export the scan the user started, at its snapshot.

## Callers changed
None: additive. The Python bindings are the first caller of `from_rows`.
