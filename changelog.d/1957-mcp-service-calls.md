### Added
- `phdb serve`: `Vectors.Nearest` takes `qualifiers` (a `QualifierSelection`: a prefix, bounds, or both) and ranks only the vectors at the qualifiers it selects, on one node and across a cluster's partitions (#1915).
- `phdb serve`: `Data.Batch` takes every write `Mutate` takes: `u64` and vector puts (vectors checked against the family's spec) and aggregate inputs, which it refused before (#1915). On a cluster mid-upgrade, a node still on the older build refuses them with `InvalidArgument`.
- `phdb serve --mcp-blocking` (default 8): how many `/mcp` messages run at once, each on a blocking thread of its own, apart from `--blocking-threads` (#1915).

### Changed
- `phdb serve`: `/mcp`'s tools run as service calls through the router, so they route, fan out and refuse as gRPC and `/v1` do. A `read_row` cursor from stdio `phdb mcp` or an older server is refused, not misread. A `feed_poll` that doesn't wait is best effort: it returns what arrives within 20 ms gaps (#1915).
