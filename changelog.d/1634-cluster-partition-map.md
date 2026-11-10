### Added
- **`phdb serve --cluster DIR --node-id ID` and `phdb cluster`: the first step of static partitions across nodes** (D317, ICR 0117; `serve` feature, in progress).
  - A node keeps the cluster's partition map and distributes it.
  - `phdb cluster init-partition` stamps a partition file; `set-map`, `show-map` and `status` set, show and check the map.
  - A new `cluster` token scope is for node-to-node calls.
  - `phdb serve` refuses table names starting with `__phdb_` and hides them from listings.
  - Serving partitioned data comes next (#1482).
