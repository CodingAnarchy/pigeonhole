### Added
- **`phdb serve`: `Graph.Traverse` and `Vectors.Nearest`** (D313 ticket 7, ICR 0112, #1397), over gRPC and HTTP/JSON. Both run without a blocking thread and read at a pinned snapshot's `snapshot_id`. `Nearest` reads a key range or a key list, and takes `page_bytes` (D308). The vector index's other RPCs answer `Unsupported` until they're served.
