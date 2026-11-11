### Added
- `phdb serve --cluster` nodes fan `Vectors.Nearest` and `Graph.Traverse` out across a table's partitions (D317 ticket 4b, ICR 0117), returning a single node's answer: the exact top `k`, and traversals with the same hops, parents and stats. `allow_partial` and `missing` work as for scans. New flag `--fanout-forwards` (8): forwards one fanned-out read runs at once.
