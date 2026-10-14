### Changed
- `phdb-bench` records D249's `CrossShardMetrics::refused_pending_overlap` with the other cross-shard refusal reasons; the gate's cross-shard rows list it, and the transfer row shows the pending-share overlaps the engine retried per transfer beside the conflicts that reached the application (ICR 0045).
