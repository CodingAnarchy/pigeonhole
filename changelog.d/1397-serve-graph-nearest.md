### Added
- **`phdb serve`: `Graph.Traverse` and `Vectors.Nearest`** (D313 ticket 7, ICR 0112, #1397), over gRPC and HTTP/JSON. Both run without a blocking thread and read at a pinned snapshot's `snapshot_id`. `Nearest` reads a key range or a key list. Both replies are byte-budgeted by `page_bytes` (D308; 1 MiB by default, 8 MiB at most), and `Nearest`'s `k` is at most 10,000.
