### Fixed
- Tablet packing no longer leaves one shard saturated beside idle ones (D251). It adds active shards when any active shard's thread stays above 90% busy, and write skew now moves small tablets off a shard that holds many equally loaded ones. On the AX102, cross-shard contention with tablet changes on had run at about half the speed of tablet changes off.
