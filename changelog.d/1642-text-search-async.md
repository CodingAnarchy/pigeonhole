### Added
- **`Search::run_async` and `Search::run_with_stats_async`** in `pigeonhole-text` (the `async` feature, default-on, #1642, #1560, ICR 0124). A text search as a future that never blocks on the device. It returns exactly what `run` does, for every query kind.
  - MaxScore's posting walks fetch their next block before each step, and skip by seeking within the row (`RowCells::poll_seek`), so a cold hub term reads only the blocks it lands in.
  - Point lookups and the once-per-query reads (statistics, fuzzy expansion, phrase positions and the hits) are futures too.
  - A search yields after a bounded amount of work, so a hub term doesn't hold an executor thread.
  - The sync `run` runs the same core.
