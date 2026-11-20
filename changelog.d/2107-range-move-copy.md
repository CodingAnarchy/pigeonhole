### Added
- **A range move's target copies its range from the source and catches up on its history** (D331, #1499), for the move driver that follows. New `phdb serve` flags:
  - `--move-rate-bytes`: a node's copy bandwidth across its moves, default 64 MiB/s.
  - `--move-retention-cap`: the most change-feed history a move keeps on its source, default 4 GiB.
  - `--max-move-followers`: moves copying from a node at once, default 2.

  New metrics: `pigeonhole_cluster_move_follower_lag_ms` and `pigeonhole_cluster_move_copies_refused_total`. `phdb cluster status` reports a move's copied bytes, whether its copy is done, and how far its catch-up has applied.
