### Fixed
- `phdb serve --cluster`: a node with several partitions and `--shards` below their count no longer reserves its compaction CPUs on a CPU one of its partitions' shards pins on. The reserve now comes after the shards the partitions get (cli692's N6 on #1718).
