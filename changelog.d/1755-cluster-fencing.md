### Added
- `phdb serve --cluster`: fencing and relocation (D317 ticket 6, ICR 0117).
  - `phdb cluster fence`, `unfence`, `close`, `relocate --to ID` and `drop-partition`.
  - A fenced partition's writes answer the new code `Fenced` through every node. A fence is kept across a restart.
  - A restarted node keeps its partitions fenced until a peer's map confirms them.
  - `close` makes a partition's files final, and `Cluster.Status` lists them with SHA-256.
  - `relocate` installs uploaded files on the new node and moves the partition there.
  - A node opens a partition the map gives it while it serves, and moves its threads off the new shards' CPUs.
  - `phdb cluster status` shows each partition's state.
