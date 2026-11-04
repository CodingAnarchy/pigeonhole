### Added
- A prototype behind a hidden option, off by default: caller-runs (#1176, D198's C2), where a thread waiting on its non-durable single-shard commit runs its idle shard's turn itself. It is under evaluation and changes nothing unless enabled.
