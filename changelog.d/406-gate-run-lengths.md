### Added
- `phdb-bench --min-secs S`: inline runs (the scaling gate and `group-commit`'s D213 run) repeat their measured writes until at least S seconds are measured. `--inline-ops N` sets the D213 run's commits apart from the client runs' (D219).

### Changed
- The gate window judges durable group commit on 2M commits and reports the 200K run beside it, measures the scaling gate for at least 8 s per shard count, and its summary breaks out each run's stalls, flushes and compactions (D219).
