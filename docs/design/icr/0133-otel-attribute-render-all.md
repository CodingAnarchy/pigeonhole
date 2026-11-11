# 0133: `pigeonhole_otel::Config::attribute` and `Collector::render_prometheus_all`

**Status:** Approved. The coordinator assigned the number (2026-11-10), for D317 ticket 8b's metrics by partition. ai1249 reviews.

## The change
- **New public names in `pigeonhole-otel`, additive** (its 0.N line: a new minor's worth, no break):
  - **`Config::attribute(self, key: &'static str, value: impl Into<String>) -> Self`:** a constant attribute every metric carries, after `pigeonhole.db.name`. It applies through OpenTelemetry (`Config::register`) and through a `Collector` (`Point::attributes`, and the Prometheus text) alike. Setting a key again replaces its value. In Prometheus text a key is a label name, as OpenTelemetry's exporter writes one: every character outside `[a-zA-Z0-9_]` becomes `_`, with `_` before a leading digit. It panics, as documented, if the key's label name is that of `pigeonhole.db.name`, of an attribute a metric carries itself, or of another key already set (`a-b` after `a.b`), since a sample would carry the label twice and Prometheus refuses the scrape (ai1249's S1 and N1 on #1711, and their follow-up). The docs say each value is one more series per metric.
  - **`Collector::render_prometheus_all(collectors: &[&Collector], out: &mut String)`:** as `render_prometheus`, for several databases at once. Each metric's `# HELP` and `# TYPE` comes once, then every collector's samples in the slice's order. Exposition 0.0.4 allows a metric's lines once, so rendering collectors one after another would repeat them. `render_prometheus` is now `render_prometheus_all(&[self], out)`, and its text is unchanged.
- **Behaviour:** none changes without the new calls. A registration or collector made without `attribute` reports exactly what it did.

## Why
D317 ticket 8a puts several partitions, each its own database, on one `phdb serve` node. Their engine metrics need telling apart by partition (ticket 8b). The file name in `pigeonhole.db.name` isn't the partition id, and an operator's dashboards group by partition. They also need rendering into one valid `/metrics` text. Both are general: any process with several databases has the same two needs, so they belong in `pigeonhole-otel`'s own types rather than in serve (simp724's review of the 8b plan).

## Callers changed
- `phdb serve` (`crates/cli/src/serve/service.rs`): a cluster node's partition builds its collector with `attribute("pigeonhole.partition", id)`, and `Service::metrics_text` renders every partition's collector with `render_prometheus_all`. A single node renders its one collector as before.
- No other caller: the rest is new API.
