### Added
- **Reader processes have a row cache** (D281, ICR 0072): `ReaderOptions::row_cache(bytes)`, 64 MiB by default as the writer's (0 turns it off), with `row_cache_max_row` and `row_cache_family`, and `PigeonholeReader::row_cache_stats()`. Hot rows read in a reader process cost about what they cost in the writer process; results are identical to uncached reads. Each reader process has its own cache, so N reader processes use up to N × 64 MiB by default. It needs the writer's row cache on (the default): a writer opened with `Options::row_cache(0)` gives its readers none.

### Changed
- **The shared-memory layout is version 5** (D281), this release's layout: the row caches' write watermarks move into the region, which grows by 2 MiB at the defaults (`memtable_budget × shards` plus about 12 MiB). **Upgrade reader processes together with the writer:** a reader from an earlier release can't attach to a writer from this one, nor the reverse (`ShmVersionMismatch`).
