### Changed
- **A cluster node opens a partition while it serves without a restart for memory** (#1761). `--engine-memory` is split again for the new set of partitions, as a restart would split it. The partitions already open keep their memtables and shrink their block and row caches to make room, before the new partition's engine opens. When a partition closes, the others' caches grow back. The open is refused only when the open partitions' memtables leave too little for the new one's, and then a restart of that node re-splits it with smaller memtables, as before. The relocation recipe no longer needs the target node restarted.

### Fixed
- **A partition opened while a cluster node serves runs its compaction on the node's compaction CPUs** (#1888). Before, its compaction threads were pinned on the free cores, where the request threads run, outside the CPUs `--compaction-threads` gave the node at start.
