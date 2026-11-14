### Changed
- **`phdb serve` cluster feeds:** subscriptions from now that enter a node with the same token and the same request share one merge there, instead of one each (#1798).
  - Each subscriber reads the shared merge's events from a ring, as on a single node. One that falls behind goes on alone from its last checkpoint, with a `--max-catch-up` slot, and rejoins once caught up.
  - A node runs at most `--max-feed-hubs` shared merges; past that, a request runs its own.
  - New metrics: `pigeonhole_serve_feed_cluster_hub_members`, `…_cluster_fell_behind_total` and `…_cluster_rejoined_total`. `pigeonhole_serve_feed_cluster_merges` now counts shared merges and subscribers' own.
  - From another node's partition, a subscription from now may also get commits made shortly before the call. From a partition the entry node serves, it gets none, as before.
