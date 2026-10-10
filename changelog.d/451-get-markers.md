### Changed
- Point gets on data in memtables that hold no delete markers skip a skiplist search (ICR 0020; get-mem −16% instructions). `pigeonhole-memtable`: `Memtable::note_marker` and `MemtableReader::may_have_markers`. `pigeonhole-compaction`: `CellResolver::seek_column_encoded_unmarked`.
