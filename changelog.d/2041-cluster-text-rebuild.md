### Added
- `phdb serve` on a cluster: `Text.Rebuild` runs on every partition of a text index and sums the counts (#1722, ICR 0138). One node's partitions rebuild in turn, and different nodes' at once. A partition that fails or can't be reached fails the call, naming it. Searches, strict ones included, go on while an index is rebuilt; only a new index's background fill answers `Unavailable`.
- `pigeonhole-text`: `Index::fill_pending()`, whether an index made with `create_unfilled` is still waiting for its first walk of the source to finish.
