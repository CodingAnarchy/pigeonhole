### Added
- **`phdb serve --max-connections N`** (#1604): a cap on open connections, HTTP/1.1 and HTTP/2 alike. Past it, a new connection is closed as soon as it's accepted (gRPC clients see `UNAVAILABLE`). The default is half the open-file limit (`ulimit -n`), at most 4096. New metrics: `pigeonhole_serve_connections`, `pigeonhole_serve_connections_max`, `pigeonhole_serve_connections_refused_total` and `pigeonhole_serve_accept_errors_total`.

### Changed
- `phdb serve` closes a connection that sends nothing within `--request-timeout-ms` of connecting, and an HTTP/1.1 request's headers must arrive within it. Out of file descriptors, accepting waits for a connection to close instead of retrying every 50 ms, and logs a line a second at most.
