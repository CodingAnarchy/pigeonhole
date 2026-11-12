### Added
- **`phdb serve`'s `/metrics` carries the engine's metrics** for the served database (`Admin.Metrics` too), under the names `pigeonhole-otel` gives them: commits and their latency, write stalls, flushes and compactions, the caches, the file, the change feed, reader slots, recoveries and the rest (D323, D313 ticket 9). Their counters keep growing across an automatic recovery and a replica's re-bootstrap. `pigeonhole_async_sync_reads_total` keeps its name and gains the `pigeonhole_db_name` label.
- **`pigeonhole_otel::Collector`** (`collector(&db)`, `Config::collector`): the same metrics without an OpenTelemetry SDK, as plain points (`collect()`) or as Prometheus text under the exporter's names (`render_prometheus`) (ICR 0131).

