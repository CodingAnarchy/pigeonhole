### Changed
- `phdb-bench scaling` splits the table at the measured writes' quantiles up front, with the balancer off, and judges efficiency alone (D231). The JSON's scaling verdict no longer depends on the busiest share.

### Added
- `phdb-bench placement`: the balancer's placement, judged on its own (D231). Skewed writes are offered open-loop at a fixed rate (`--offered-rate`, default 30,000 writes/s per shard) over engine-owned shards. It passes when the table settles within 60 s, nothing splits, merges or moves while measuring, the busiest shard takes at most 1.25/N of the writes, and the offered rate is sustained. The JSON carries it as `placement`.
