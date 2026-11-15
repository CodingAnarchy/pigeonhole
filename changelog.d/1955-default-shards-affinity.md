### Fixed
- `phdb serve`: a database without `--shards` now gets half the physical cores the process may run on (its CPU affinity and quota), not half the host's (#1921). In a restricted container or under `taskset` it no longer gets more shards than it has CPUs for. A database with a change feed keeps the count its feed recorded.
