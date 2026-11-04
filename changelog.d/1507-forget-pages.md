### Fixed
- `pigeonhole-memory`'s forgetting pass reads its scan in pages of 256 rows, each with its own short-lived view, instead of one iterator for the whole pass. Over a large store, the long-lived view kept flushed memtables from being reused, so writers could wait for memtable room while a pass ran (#1507, from #1477 and #1506).
