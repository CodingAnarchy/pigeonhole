### Added
- `Nearest::run_async` (D296): a nearest-neighbour search as a future that reads its rows on the polling task, with their blocks fetched asynchronously, and yields every 4,096 cells, so a wide row never holds an executor's thread. Its result equals `run`'s.
