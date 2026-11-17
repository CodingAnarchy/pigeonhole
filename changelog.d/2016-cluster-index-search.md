### Added
- `phdb serve --cluster`: `Vectors.Search`, `Vectors.Verify` and `Text.Verify` across an index's partitions (#1722, ICR 0138).
  - A search goes to each partition its `id_prefix` meets (every partition without one), and the node called keeps the first `k` by distance, then id.
  - `nprobe` and `max_probe` are per partition, so recall and cost rise with the partitions. `stats` sums the partitions' lists.
  - A partition that can't be reached fails the search with `PartitionUnavailable`.
  - `Verify` sums every partition's counts, and a partition whose check fails is named in the error.
  - `Text.Search`, `Text.Rebuild` and `Vectors.BulkLoad` across partitions come in later PRs.
