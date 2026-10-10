### Changed
- Reads that do not fill the block cache (compaction, and scans with `fill_cache` off) reuse a few decompression buffers per SST instead of allocating and zero-filling one per compressed block (#372). Pieces of merged readahead runs come from the same buffers.

### Added
- `pigeonhole-io`: `BufPool`, a bounded pool of `IoBuf`s that go back to it when dropped (ICR 0026).
