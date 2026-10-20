### Deprecated
- `CrossShardMetrics::refused_below_floor` and `refused_move_raise`: always 0 since 0.4.0, which picks a cross-shard commit's timestamp at its decision (D263), so no participant refuses a round for its timestamp. They are removed at 1.0.
