### Changed
- `phdb-bench`: after a run switches from its buffered load to a durable level, the warmup waits for 5 s without a placement change (not 2) and at most 60 s (not 30), so packing's re-placement for the new level no longer lands in the measured phase (D242, D247).
