### Added
- `pigeonhole-text`: `Index::over_table(db, table)` lists the text indexes whose source column is in a table (#1916).

### Changed
- `phdb serve`: `Schema.DropTable` of a text index's source table is refused with `FailedPrecondition`, naming the indexes (also while a `Text.CreateIndex` over it is building). In 1.2 the drop succeeded and left the index unable to write, rebuild or find its documents, with a new table of the same name becoming its source. Drop the indexes first (#1916).
