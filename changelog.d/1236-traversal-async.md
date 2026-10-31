### Added
- Async traversal (D304): `Traverse::run_async()` runs a traversal without blocking on the device. Each hop's rows are positioned from the block cache, and the blocks they and the next keys need are fetched asynchronously. A poll yields after 4,096 edges, so a hub never holds an executor thread for long. Its result equals `run()`'s.
