### Changed
- `phdb-bench group-commit`'s D213 run now keeps its pre-split placement fixed: the balancer is off, and the run fails unless every shard owns one tablet. A tablet change while measuring makes the bench exit non-zero. The run also reports the engine's part of the commit latency, WAL rollover blocks and inline WAL syncs.
