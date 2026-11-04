# pigeonhole-otel

Exports a [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) database's engine metrics through OpenTelemetry, so they reach Prometheus, Grafana or any OTLP backend beside your application's own metrics (D316).

```rust
use opentelemetry::metrics::MeterProvider as _;
use pigeonhole::{Options, Pigeonhole};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
# let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
let meter = provider.meter("my-app");
// Keep it: dropping the registration stops reporting.
let _metrics = pigeonhole_otel::register(&db, &meter);
# drop(_metrics);
# db.close()?;
# Ok(())
# }
```

Every instrument is observable: nothing runs until your exporter collects, and commits and reads pay nothing. The metrics, what to alert on, and wiring to Prometheus or an OTLP collector are in the [observability guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/observability.md).

**Versions:** this crate has its own 0.N line, outside `pigeonhole`'s 1.x line, because its API takes `opentelemetry` types: each `opentelemetry` minor is a new minor here. The metric names are a stable contract on their own (D316).

License: MIT.
