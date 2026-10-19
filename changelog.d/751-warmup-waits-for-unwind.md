### Changed
- `phdb-bench`'s warm-up also waits while fewer shards hold a tablet than the balancer's active count (D242's second amendment): a growth's moves come one per dwell and a 2 s gap between two of them ended the warm-up inside the regrow in 2 of 6 gate reps (228–236K ops/s on the cross-shard row).
