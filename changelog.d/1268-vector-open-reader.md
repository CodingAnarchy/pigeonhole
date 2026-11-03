### Added
- `pigeonhole-vector`: `Index::open_reader(&PigeonholeReader, name)` opens an IVF index from a reader process as a search-only `ReadIndex`, with the same `search_*` builders, at the reader's snapshots (#1268, ICR 0100).
