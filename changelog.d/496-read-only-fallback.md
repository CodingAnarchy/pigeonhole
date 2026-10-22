### Added
- `Options::read_only_fallback` (ICR 0059, #496). A writer open of a database this build can't write opens read-only instead of failing. That covers a feature from a newer version that older ones may only read (otherwise `UnsupportedFeature`), and a family naming an unregistered merge operator (otherwise `UnknownMergeOperator`). Such an open writes nothing to the database's files:
  - no format upgrade, flush, compaction or clean-close mark;
  - the write-ahead log is replayed into memory, and an open whose log doesn't fit `memtable_budget` fails with `ReadOnly`;
  - `flush`, `compact` and `shrink` fail with `ReadOnly`, as writes do.
- `DatabaseInfo::features` (a new `FileFeatures` struct) and `DatabaseInfo::read_only`. `phdb info` shows both.

### Changed
- `DatabaseInfo::format_version` is the file's version: a read-only open no longer reports this build's.
- Creating a family that names an unregistered merge operator is refused whatever the options.

### Deprecated
- `Options::allow_unregistered_merge_operators`: use `read_only_fallback`, which also covers it. Its read-only handle no longer compacts or flushes the database's other families.
