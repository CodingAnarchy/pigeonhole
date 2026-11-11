### Added
- `phdb serve --cluster` serves transactions and pinned snapshots (D317 ticket 3, ICR 0117). Each lives on the node serving its partition, named by `route` in `Begin` or `Snapshots.Open` (required on a cluster), and its id names the partition, so any node routes the calls on it. A row outside the partition is refused with `CrossPartition` before anything is written.

### Changed
- `phdb serve`'s transactions and snapshots belong to their token's SHA-256 digest rather than its line in the token file, so a reordered or reloaded token file keeps them.
