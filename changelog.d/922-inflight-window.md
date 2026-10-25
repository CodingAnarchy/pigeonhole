### Fixed
- The tablet balancer's commits-in-flight measure (D257) counts a commit only for its part inside the interval it ends in. A commit whose sync outlasted several balancer intervals used to count all of its time in the last one, so one blocking client could read as several commits in flight and skew the shard-packing decision (#922).
