# 0052: `Scan::into_owned`

**Status:** Approved (owner, via the coordinator, 2026-10-21; #691, D1). Additive.

## Change

In `pigeonhole`:
- `Scan::into_owned(self) -> Scan<'static>`

The scan keeps its selection (bounds, families, qualifier filters, versions, time range, value filter, snapshot, limit) and holds the table's `Arc` instead of borrowing the handle. `Scan<'static>::iter` gives a `RowIter<'static>`, and `stream` (the `async` feature) a `RowStream<'static>`. Both hold the table the same way, so an owned iterator keeps the database open exactly as a clone of the `Table` or `ReadTable` handle would. `RowIter` and `RowStream` were already `Send`; nothing below the public crate changes.

Internally, `Scan`, `RowIter` and `RowStream` keep the table as a `Cow<'t, Arc<TableCore>>` in place of `&'t TableCore` and `PhantomData`. A borrowed scan is unchanged.

## Why

The Arrow export (#691) hands a scan to DuckDB or Polars as an Arrow C stream (`FFI_ArrowArrayStream`), which takes a `Box<dyn RecordBatchReader + Send>`: the reader must be `'static`. A `RowIter<'t>` borrows its table handle, so without this the export would need a self-referential struct holding the handle and its own borrow. `pigeonhole-arrow` is `forbid(unsafe_code)`, and such a struct needs `unsafe` or a dependency. The same need comes up for anything that keeps a scan beyond the caller's stack frame: a worker thread, a server's open cursor, a language binding.

Alternatives considered: making `RowIter` always own the table (a breaking change of its lifetime parameter, for no gain to borrowed scans), or `Table::scan_owned` and friends (three new methods on each of two handles, where one on `Scan` covers every way of starting a scan).

## Callers

- `pigeonhole`: `read.rs` (`Scan`, `RowIter`), `nonblocking.rs` (`RowStream`), `table.rs` (the scan constructors take `&Arc<TableCore>`), and `tests/api.rs` (`an_owned_scan_outlives_its_table_handle`: the selection carries over, the iterator moves to another thread after the handle drops, on the writer and on a reader process; `Send + 'static` asserted for `RowIter` and `RowStream`).
- `pigeonhole-arrow`: `ArrowScan<'static>` and its `ffi` stream (#691, PRs 2 and 3).
- Docs: the agent reference, the rustdoc example, and the changelog.
