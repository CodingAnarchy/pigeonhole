### Added
- The Phase 3 gate's `contended` step (D247): 16 client threads on the default options, runs of at least 60 s, against RocksDB and SQLite at D108's buffered parity and at the default durability (`GroupSync` against fsync per write), both judged within 1.5× and shown side by side; cross-shard-contention gets D246's checks at both levels.
