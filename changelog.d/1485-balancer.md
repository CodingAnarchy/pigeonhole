### Added
- **A cluster balancer, off by default** (D332, #1485). With the map's `balancer` ON, the map's writer moves whole partitions between nodes when one node's load or size per unit of weight stays above the mean for long enough, one move at a time, with a rest after each. A partition that keeps moving is pinned.
  - `phdb cluster balancer status|pause|resume|pin|unpin` controls it.
  - `/metrics` adds `pigeonhole_cluster_balancer_actions_total` and `pigeonhole_cluster_node_load_ratio`.
  - New map fields: `balancer`, `balance`, `balancer_events`, `Partition.pinned` and `moves_history`, `TablePlacement.pinned_ranges` and `Node.weight`. Also `LocalPartition.file_bytes`, `NodeStatus.balancer` and `StatusRequest.open_only`.
  - Splitting and merging partitions come with the online split.
