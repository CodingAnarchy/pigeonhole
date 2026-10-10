### Changed
- **Reads skip superseded memtable versions, by default** (D194, D199, ICR 0013, #387). The writer keeps a process-local stale-tail index beside each memtable, so row reads and scans pass a column's older memtable versions in one jump instead of stepping each. On Linux callgrind: ycsb-c −43%, ycsb-a −35%, a heavily overwritten hot row −19%. An overwrite costs about 200 more instructions to record (D199). Results are identical. Reader processes and the file format are unchanged. `EngineOptions::memtable_tail_index` turns it off.

### Added
- `Cursor::skip_column` (`pigeonhole-format`, a provided method defaulting to no skip), `Memtable::with_tail_index` and `MemIter::skips_columns` (`pigeonhole-memtable`), `ResolveOptions::skip_columns` (`pigeonhole-compaction`) and `EngineOptions::memtable_tail_index` (`pigeonhole-engine`) (ICR 0013).
