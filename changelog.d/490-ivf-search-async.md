### Added
- `pigeonhole-vector`: `Search::run_async` runs an index search without blocking on the device. The centroids' version and a changed layout are read asynchronously, and the probed lists through `Nearest::run_async`. Its result equals `run`'s.
