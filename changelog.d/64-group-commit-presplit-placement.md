### Fixed
- `phdb-bench group-commit`'s D213 run keeps one tablet per shard at high shard counts: every shard spends its single balancer pass before the pre-split, so none consolidates its new piece onto a neighbour. The gate window also records the commit each step ran at, and refuses to resume on binaries built at another commit.
