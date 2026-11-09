### Fixed
- **`phdb serve` on a replica no longer keeps a stale table, vector index or text index handle** (#1632). A handle opened while the replica applied a catalog change (a drop and create of the same name), or swapped in a new copy, could be served until the next catalog change. Such an open now keeps nothing and opens again.
