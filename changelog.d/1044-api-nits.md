### Changed
- **Breaking (before the 1.0 API freeze; ICR 0086):**
  - **Compression:** `Family::lz4()`, `zstd(level)` and `uncompressed()` become one `Family::compression(Compression)`: `Compression::Lz4` (the default), `Compression::Zstd(level)` or `Compression::None`.
  - **Column limit:** `Scan::columns_per_row(n)` is now `Scan::column_limit(n)`, the same name as `RowRead`'s; both cap columns per family. `phdb`'s `columns-per-row` argument is unchanged.
  - **`Row::entry(i)`** returns a `CellEntry`, as `RowRef::entry` does.
  - **`repr(C)`:** `Failure` and `Recovery` are no longer `#[repr(C)]`.
  - **The `phdb` library target** (`pigeonhole_cli::run`, `ExitStatus`) is hidden; the promise covers the binary.

### Removed
- **Breaking (ICR 0086):** `Pigeonhole::async_sync_reads()`. Read `engine_metrics().async_sync_reads`; `PigeonholeReader::async_sync_reads()` stays.
