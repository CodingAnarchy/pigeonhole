### Fixed
- Cluster failover and `phdb cluster promote --old-unreachable` no longer lose writes that an owner acknowledged after coming back mid-promote (#2152).
  - No node grants a partition's lease to its owner while the map holds a pending promote of that partition.
  - The promote's first map must reach a majority before the replica is stamped.
  - Surveys of the partition's grants before and after that map must find none to the owner, within a lease of each other. Otherwise the promote is abandoned before its stamp, and the replica returns to `replicas`.
  - `--old-unreachable` first waits out grants from before the owner stopped, for up to two leases.
  - The map's writer abandons a pending promote once its owner asks it for the lease and answers waiting, so the owner serves again.
  - `NodeStatus.lease_ms` (field 18) reports each node's `--cluster-lease-ms` for the CLI's check.
