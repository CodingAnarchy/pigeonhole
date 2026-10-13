### Changed
- `phdb-bench scaling` and `group-commit`'s D213 run pin each shard's thread to a CPU of its own (D235), judged, and report the same runs unpinned beside them (`scaling_unpinned`, `group_commit_unpinned`).
