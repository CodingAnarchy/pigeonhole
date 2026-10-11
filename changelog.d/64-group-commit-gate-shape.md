### Changed
- `phdb-bench group-commit` on Pigeonhole first runs the shape the durable group-commit target is judged on (D213). It uses one shard per core, splits the table over every shard before the load, and has each shard's thread keep 16 durable commits in flight. The report shows cells/s in total and per core, and commit p50 and p99. The 1, 4 and 16 client-thread runs follow, as before.
