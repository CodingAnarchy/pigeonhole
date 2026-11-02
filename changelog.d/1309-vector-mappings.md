### Fixed
- `phdb dump` no longer loses vector cells (#1309, D303 PR 3): they're written as `{"vector": {"dim", "element", "values"}}` (bit vectors as `base64`), exact for every element, and `phdb load` writes them back bit for bit, checked against the family. A vector family's options carry `"vector": {"dim", "element", "metric"}` in `dump`, `load`, `phdb info` and the MCP tools.
- An Arrow export (`pigeonhole-arrow`, `phdb dump --format arrow`) no longer fails on a vector cell: `value_type` is `vector` and `value` its stored encoding.

### Added
- `ArrowOptions::vector_dim(n)` (ICR 0108): a `value_vector: FixedSizeList<Float32, n>` column of each vector cell's components, widened exactly.
