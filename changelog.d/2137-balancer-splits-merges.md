### Added
- **The cluster balancer splits and merges partitions** (D332, #1485). It runs D331's online split and merge under `bal-` records:
  - **Splits:** a partition too big or too hot is split at the tablet boundary that halves its load, onto a new partition `<P>-<v>` on the coldest node free of its replicas.
  - **Merges:** a small, cold partition merges into a neighbour, then its emptied partition is dropped once nothing still needs it.
  - **Survey:** a pass asks the candidate's owner for its tablets through `Forward`'s new `tablets` arm.
  - **Copy time:** a split's copy must fit in the dwell at the source's `--move-rate-bytes`.
  - **When the writer changes:** a balancer range move or split left behind by an earlier writer, or one whose run stopped, is aborted before its hand-over and finished after it.
  - `actions_total{kind}` gains `split` and `merge`.
  - **Status:** while the balancer is paused, its `state` still names an emptied partition's removal that waits or stopped (#2138).

### Changed
- `--move-rate-bytes` must be at least 1.
