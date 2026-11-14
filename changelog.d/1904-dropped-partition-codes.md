### Fixed
- `phdb serve` (cluster): a partition a forced map drops no longer answers `Internal`. A change feed's part of it ends with `PartitionUnavailable` naming the drop, so an `allow_partial` subscription reports it and goes on with the other partitions. A fanned-out read's part of it answers `WrongPartition` with the map's version (#1896).
