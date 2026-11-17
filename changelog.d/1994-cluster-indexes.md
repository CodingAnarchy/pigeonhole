### Added
- `phdb serve --cluster`: vector and text indexes on a cluster (#1722, ICR 0138).
  - `Vectors.CreateIndex` and `Text.CreateIndex` through any node place the index in the cluster map, and each partition it's placed on holds an index of its own range.
  - A text index follows its source table, and on a cluster it fills from its source rows in the background. A vector index follows a table (`follows`) or has ranges of its own (`placement`).
  - `Insert`/`Delete` and `Put`/`Delete` route to the partition holding the id, and `Schema.DropTable` of the index's name drops it everywhere.
  - A table an index follows can't be dropped while the index is there.
  - The writer adds the first index only once every node runs a build that keeps them (`indexes` in `map_features`).
  - Searches, `Verify`, `Rebuild` and `BulkLoad` across an index's partitions come in later PRs.
- `pigeonhole-text`: `Index::create_unfilled` creates an index with its backfill left as a pending rebuild, for `resume_rebuild_with` to run in steps (#1722).
