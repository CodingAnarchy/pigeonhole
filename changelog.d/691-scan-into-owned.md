### Added
- `Scan::into_owned`: a scan that holds its table instead of borrowing it (ICR 0052). Its `RowIter` (and its `RowStream`, with the `async` feature) is `Send + 'static`, so it can move to another thread or into a consumer that keeps it, such as the Arrow export coming in `pigeonhole-arrow` (#691). It keeps the database open as a clone of the table handle would.
