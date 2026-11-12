### Fixed
- **Memory is returned after a flush even when the shard is idle** (#1600). A flush could report done while still holding the memtable it had written. If the shard handled the report first, the memtable's arena memory stayed allocated until the next write needed room. The flush now lets go of the memtable before it reports.
