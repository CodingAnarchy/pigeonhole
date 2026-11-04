### Added
- **`phdb serve`: `Data.Scan`** (D313 ticket 3b, ICR 0112, #1397). A scan streams byte-budgeted pages: a gRPC server stream, or over HTTP one JSON page per line (NDJSON) or server-sent events.
  - A scan that fails partway ends with a final error message.
  - Its row `limit` counts each row once, even when a page cuts inside a row.
  - `--max-scans` (default 64) caps the scans streaming at once; more are refused with `Overloaded`.
- **`phdb serve` request durations:** `/metrics` adds `pigeonhole_serve_request_duration_seconds`, a histogram per RPC.

### Changed
- **`phdb serve` reserves the in-flight budget before reading a gRPC message** (from its header), as it already did over HTTP from `Content-Length`. A client's `grpc-timeout` bounds that wait (`DeadlineExceeded`).
- **`phdb serve`: a read that fails on `Io` or `MergeFailed`** now says it may be sent again (`retry_from: REQUEST`). A write's failure still says its outcome is unknown.
