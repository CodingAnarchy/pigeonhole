### Added
- `pigeonhole-format`: `ScanFilter::compile` returns a `CompiledFilter`, whose `check` decides admission and the seek hint with one key decode and escaped byte comparisons (ICR 0030). SST and memtable cursors use it for filtered scans (#29).
