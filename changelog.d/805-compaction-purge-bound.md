### Fixed
- A compaction triggered by a flush no longer keeps every version that flush wrote. It used to count the flushed memtable as a source above its inputs until the shard finished handling the flush, so a family with `max_versions`, or rows with deletes, kept the extra versions and markers until a later compaction rewrote them (#805).
