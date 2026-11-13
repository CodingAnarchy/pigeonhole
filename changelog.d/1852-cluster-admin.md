### Added
- **`Admin.Info`, `Admin.Flush` and `Admin.Compact` work on a cluster node** (#1852, ICR 0117 amendment). In 1.2.0 they answered `Unsupported` there.
  - `Info` lists each partition the node holds, with its file, size, tables, and state (as `phdb cluster status` shows it), and names the map's other partitions.
  - `Flush` and `Compact` act on every partition open on the node, or on the one their new `partition` field names.
  - Each node answers for its own partitions.
