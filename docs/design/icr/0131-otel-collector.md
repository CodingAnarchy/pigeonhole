# 0131: `pigeonhole_otel::Collector`, and `phdb serve --affinity`

**Status:** Approved. The coordinator assigned the number (2026-11-10) with D323, for D313 ticket 9: the collector (option B of the ticket's plan) in ticket 9a, and the affinity switch in 9b.

## The change
- **New public names in `pigeonhole-otel`, additive** (its 0.N line: a new minor's worth, no break):
  - `pigeonhole_otel::collector(&Pigeonhole) -> Collector`, and `Config::collector(self, &Pigeonhole) -> Collector`: a reading of the same metrics `register` reports, with no OpenTelemetry type in its API.
  - `Collector::collect(&self) -> Vec<Point>`: each metric's points now, in `metrics()`' order.
  - `Collector::render_prometheus(&self, &mut String)`: the same as Prometheus text (exposition 0.0.4): `# HELP` (the metric's description), `# TYPE`, and one sample per point, named by `MetricInfo::prometheus_name`, labelled by the attributes with dots as underscores, in name order. This is what `opentelemetry-prometheus` writes without its scope labels and `target_info`. A test checks it against the exporter's own text for the same database.
  - `Collector::rebind` and `Collector::watch_feed`, as `Registration`'s.
  - `Point { metric: MetricInfo, value: PointValue, attributes: Vec<(&'static str, String)> }` and `PointValue { U64(u64), F64(f64) }`, both `#[non_exhaustive]`. `attributes` has `pigeonhole.db.name` first, then the point's own; a level is written `0`, a flag `true`.
- **Behaviour:**
  - A collector and a registration share one implementation: one reading per collection (reused for 100 ms), counters carried across automatic recoveries and `rebind`, and nothing once the database is closed.
  - **Fixed in passing:** a registration's reading cached within its 100 ms reuse used to outlive `close()`. Both now report nothing once the database is closed.
- **`phdb serve`:**
  - `/metrics` and `Admin.Metrics` append the served database's metrics through a `Collector` (D323).
  - `pigeonhole_async_sync_reads_total` keeps its name, now with the `pigeonhole_db_name` label every engine metric carries.
  - `--affinity on|off` (default `on`) comes with ticket 9b: off keeps the thread plan's counts but sets no CPU mask, for shared hosts and for R1's acceptance run.

## Why
D313 ticket 9 puts the engine's metrics on `/metrics` under D316's names. A second hand-written mapping in serve would drift from `pigeonhole-otel`'s table. An OpenTelemetry SDK and the `prometheus` crate in every serve build would add a second registry beside serve's own hand-rendered metrics. The collector keeps one table and one reading for both outputs.

## Callers changed
- `phdb serve` (`crates/cli/src/serve/service.rs`): `Service::metrics_text` appends the collector's text, and `Metrics::render` no longer writes the engine's `async_sync_reads`.
- No other caller: the rest is new API.
