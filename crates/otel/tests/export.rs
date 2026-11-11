//! `pigeonhole-otel` through a real OpenTelemetry SDK (D316): every documented metric is
//! reported, with its documented kind, unit and attributes, after a workload that exercises
//! it; a dropped registration reports nothing; counters continue across a reopen; the
//! default cardinality doesn't grow with the schema; and the Prometheus exporter's names are
//! the ones `MetricInfo::prometheus_name` documents.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use pigeonhole::{Durability, ErrorCode, Family, FeedRetention, Options, Pigeonhole, Table};
use pigeonhole_otel::{Config, InstrumentKind, MetricInfo};

/// One collected point: its value and attributes.
#[derive(Debug, Clone)]
struct Point {
    value: f64,
    attrs: BTreeMap<String, String>,
}

/// One collected metric.
#[derive(Debug)]
struct Collected {
    kind: InstrumentKind,
    unit: String,
    points: Vec<Point>,
}

struct Sdk {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Sdk {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        // Collections happen when the test flushes; the interval only has to be long.
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        Self { provider, exporter }
    }

    fn meter(&self) -> opentelemetry::metrics::Meter {
        self.provider.meter("pigeonhole-otel-test")
    }

    /// One collection, by metric name.
    fn collect(&self) -> BTreeMap<String, Collected> {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        let mut out = BTreeMap::new();
        for rm in self.exporter.get_finished_metrics().unwrap() {
            for scope in rm.scope_metrics() {
                for m in scope.metrics() {
                    let (kind, points) = points(m.data());
                    out.insert(
                        m.name().to_owned(),
                        Collected {
                            kind,
                            unit: m.unit().to_owned(),
                            points,
                        },
                    );
                }
            }
        }
        out
    }
}

fn attrs<'a>(kvs: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> BTreeMap<String, String> {
    kvs.map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}

fn points(data: &AggregatedMetrics) -> (InstrumentKind, Vec<Point>) {
    fn of<T: Copy + Into<f64> + std::fmt::Debug>(
        d: &MetricData<T>,
    ) -> (InstrumentKind, Vec<Point>) {
        match d {
            MetricData::Gauge(g) => (
                InstrumentKind::Gauge,
                g.data_points()
                    .map(|p| Point {
                        value: p.value().into(),
                        attrs: attrs(p.attributes()),
                    })
                    .collect(),
            ),
            MetricData::Sum(s) => {
                assert!(s.is_monotonic(), "a sum that isn't monotonic");
                (
                    InstrumentKind::Counter,
                    s.data_points()
                        .map(|p| Point {
                            value: p.value().into(),
                            attrs: attrs(p.attributes()),
                        })
                        .collect(),
                )
            }
            other => panic!("unexpected aggregation {other:?}"),
        }
    }
    match data {
        AggregatedMetrics::F64(d) => of(d),
        AggregatedMetrics::U64(d) => {
            // u64 has no lossless `Into<f64>`; the test's values are small.
            let (kind, pts) = match d {
                MetricData::Gauge(g) => (
                    InstrumentKind::Gauge,
                    g.data_points()
                        .map(|p| (p.value(), attrs(p.attributes())))
                        .collect::<Vec<_>>(),
                ),
                MetricData::Sum(s) => {
                    assert!(s.is_monotonic(), "a sum that isn't monotonic");
                    (
                        InstrumentKind::Counter,
                        s.data_points()
                            .map(|p| (p.value(), attrs(p.attributes())))
                            .collect(),
                    )
                }
                other => panic!("unexpected aggregation {other:?}"),
            };
            (
                kind,
                pts.into_iter()
                    .map(|(v, attrs)| Point {
                        value: v as f64,
                        attrs,
                    })
                    .collect(),
            )
        }
        AggregatedMetrics::I64(_) => panic!("pigeonhole-otel reports no i64 instrument"),
    }
}

fn options() -> Options {
    Options::default()
        .shards(1)
        .pin_threads(false)
        .row_cache(1 << 20)
        .change_feed(FeedRetention::bytes(8 << 20))
}

fn table(db: &Pigeonhole, name: &str) -> Table {
    db.table(name)
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

/// Commits at every durability, transactions (one conflicting), flushes, a compaction, and
/// repeated reads, so that every counter the workload can move has moved.
fn workload(db: &Pigeonhole, t: &Table) {
    for (i, d) in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ]
    .into_iter()
    .enumerate()
    {
        for j in 0..20 {
            t.mutate(format!("r{i}-{j}").as_bytes())
                .put("f", b"q", b"value")
                .durability(d)
                .commit()
                .unwrap();
        }
    }
    let mut a = db.transaction().unwrap();
    let mut b = db.transaction().unwrap();
    a.get(t, b"r2-0", "f", b"q").unwrap();
    b.get(t, b"r2-0", "f", b"q").unwrap();
    a.put(t, b"r2-0", "f", b"q", b"a");
    b.put(t, b"r2-0", "f", b"q", b"b");
    a.commit().unwrap();
    assert_eq!(b.commit().unwrap_err().code(), ErrorCode::Conflict);
    db.flush().unwrap();
    for j in 0..20 {
        t.mutate(format!("r2-{j}").as_bytes())
            .put("f", b"q2", b"value")
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.compact().unwrap();
    for _ in 0..3 {
        for j in 0..20 {
            t.get(format!("r2-{j}").as_bytes(), "f", b"q").unwrap();
        }
    }
}

fn info(name: &str) -> MetricInfo {
    pigeonhole_otel::metrics()
        .find(|m| m.name == name)
        .unwrap_or_else(|| panic!("{name} isn't a documented metric"))
}

fn total(c: &Collected) -> f64 {
    c.points.iter().map(|p| p.value).sum()
}

/// Metrics a workload on this platform may leave without values: io_uring's off Linux's
/// io_uring backend, and feed lag without a watched feed.
fn may_be_empty(db: &Pigeonhole, name: &str) -> bool {
    (name.starts_with("pigeonhole.io.") && db.io_rings().is_none())
        // Reported only for the feeds a test watches.
        || name == "pigeonhole.feed.lag"
}

#[test]
fn every_documented_metric_is_reported_with_its_kind_unit_and_attributes() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("app.phdb"), options()).unwrap();
    let t = table(&db, "t");
    let sdk = Sdk::new();
    let metrics = pigeonhole_otel::register(&db, &sdk.meter());
    let feed = t.feed().open().unwrap();
    metrics.watch_feed("indexer", feed.progress());
    workload(&db, &t);
    let got = sdk.collect();
    drop(feed);
    for m in pigeonhole_otel::metrics() {
        // The SDK leaves an instrument without values out of the export entirely.
        if may_be_empty(&db, m.name) && !got.contains_key(m.name) {
            continue;
        }
        let c = got
            .get(m.name)
            .unwrap_or_else(|| panic!("{} wasn't exported", m.name));
        assert_eq!(c.kind, m.kind, "{}", m.name);
        assert_eq!(c.unit, m.unit, "{}", m.name);
        if may_be_empty(&db, m.name) {
            continue;
        }
        assert!(!c.points.is_empty(), "{} has no values", m.name);
        for p in &c.points {
            assert_eq!(
                p.attrs.get("pigeonhole.db.name").map(String::as_str),
                Some("app.phdb"),
                "{}",
                m.name
            );
            for key in p.attrs.keys() {
                assert!(
                    key == "pigeonhole.db.name" || m.attributes.contains(&key.as_str()),
                    "{}: undocumented attribute {key}",
                    m.name
                );
            }
        }
    }
    // The workload's own counts.
    let commits = &got["pigeonhole.commits"];
    let group_sync = commits
        .points
        .iter()
        .find(|p| p.attrs["pigeonhole.durability"] == "group_sync")
        .unwrap();
    assert!(group_sync.value >= 40.0, "{group_sync:?}");
    let txns = &got["pigeonhole.transactions"];
    let outcome = |o: &str| {
        txns.points
            .iter()
            .find(|p| p.attrs["pigeonhole.outcome"] == o)
            .unwrap()
            .value
    };
    assert_eq!((outcome("committed"), outcome("conflict")), (1.0, 1.0));
    assert!(total(&got["pigeonhole.flushes"]) >= 2.0);
    assert!(total(&got["pigeonhole.block_cache.requests"]) > 0.0);
    // Engine-side latency: seconds, and plausible.
    for p in &got["pigeonhole.commit.engine_latency"].points {
        assert!(p.value > 0.0 && p.value < 10.0, "{p:?}");
    }
    assert!(
        got["pigeonhole.commit.engine_latency"]
            .points
            .iter()
            .any(|p| p.attrs["pigeonhole.durability"] == "sync"
                && p.attrs["pigeonhole.quantile"] == "0.99")
    );
    assert_eq!(total(&got["pigeonhole.failed"]), 0.0);
    drop(metrics);
    drop(t);
    db.close().unwrap();
}

/// D316 A4: a watched feed's lag is the commits it hasn't read. It grows while the
/// subscriber doesn't read, and drops to 0 once it reads everything. Writes to a table the
/// feed doesn't carry leave a feed that keeps reading at 0, since the reader's position
/// moves past them too. A dropped feed stops reporting.
#[test]
fn a_watched_feed_reports_its_lag() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("lag.phdb"), options()).unwrap();
    let fed = table(&db, "fed");
    let other = table(&db, "other");
    let sdk = Sdk::new();
    let metrics = pigeonhole_otel::register(&db, &sdk.meter());
    let mut feed = fed.feed().open().unwrap();
    metrics.watch_feed("indexer", feed.progress());
    let lag = |sdk: &Sdk| -> Option<f64> {
        // Past the 100 ms a reading is reused for, so each collection reads afresh.
        std::thread::sleep(Duration::from_millis(150));
        let got = sdk.collect();
        let c = got.get("pigeonhole.feed.lag")?;
        let p = c
            .points
            .iter()
            .find(|p| p.attrs.get("pigeonhole.feed.name").map(String::as_str) == Some("indexer"))?;
        Some(p.value)
    };
    let commit = |t: &Table, i: u32| {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", b"v")
            .durability(Durability::GroupSync)
            .commit()
            .unwrap();
    };
    for i in 0..10 {
        commit(&fed, i);
    }
    assert!(lag(&sdk).unwrap() >= 10.0, "an unread feed lags");
    let mut read = 0;
    while feed.try_next().unwrap().is_some() {
        read += 1;
    }
    assert_eq!(read, 10);
    assert_eq!(lag(&sdk), Some(0.0), "a feed that read everything");
    for i in 0..10 {
        commit(&other, i);
    }
    assert!(
        feed.try_next().unwrap().is_none(),
        "nothing for the feed's table"
    );
    assert_eq!(lag(&sdk), Some(0.0), "writes to another table, read past");
    drop(feed);
    assert_eq!(lag(&sdk), None, "a dropped feed stops reporting");
    drop(metrics);
    drop((fed, other));
    db.close().unwrap();
}

#[test]
fn a_dropped_registration_or_a_closed_database_reports_nothing() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("drop.phdb"), options()).unwrap();
    let t = table(&db, "t");
    let sdk = Sdk::new();
    let metrics = pigeonhole_otel::register(&db, &sdk.meter());
    t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();
    assert!(!sdk.collect()["pigeonhole.commits"].points.is_empty());
    drop(metrics);
    let after = sdk.collect();
    assert!(
        after.values().all(|c| c.points.is_empty()),
        "values after the drop: {after:?}"
    );
    // A registration over a database that closes stops too.
    let sdk = Sdk::new();
    let metrics = pigeonhole_otel::register(&db, &sdk.meter());
    drop(t);
    db.close().unwrap();
    let after = sdk.collect();
    assert!(
        after.values().all(|c| c.points.is_empty()),
        "values after the close: {after:?}"
    );
    drop(metrics);
}

#[test]
fn rebind_keeps_counters_monotonic_across_a_reopen() {
    let dir = pigeonhole::doc_support::temp_dir();
    let path = dir.join("re.phdb");
    let db = Pigeonhole::open(&path, options()).unwrap();
    let t = table(&db, "t");
    let sdk = Sdk::new();
    let metrics = pigeonhole_otel::register(&db, &sdk.meter());
    for j in 0..10 {
        t.mutate(format!("r{j}").as_bytes())
            .put("f", b"q", b"v")
            .commit()
            .unwrap();
    }
    let before = total(&sdk.collect()["pigeonhole.commits"]);
    assert!(before >= 10.0);
    drop(t);
    db.close().unwrap();
    let db = Pigeonhole::open(&path, options()).unwrap();
    metrics.rebind(&db);
    let t = table(&db, "t");
    t.mutate(b"after").put("f", b"q", b"v").commit().unwrap();
    let after = total(&sdk.collect()["pigeonhole.commits"]);
    assert!(after >= before + 1.0, "{before} then {after}");
    drop(metrics);
    drop(t);
    db.close().unwrap();
}

#[test]
fn default_cardinality_is_independent_of_the_schema() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("card.phdb"), options()).unwrap();
    let tables: Vec<Table> = (0..6).map(|i| table(&db, &format!("t{i}"))).collect();
    for t in &tables {
        t.mutate(b"r").put("f", b"q", b"v").commit().unwrap();
    }
    db.flush().unwrap();
    let sdk = Sdk::new();
    let by_level = pigeonhole_otel::register(&db, &sdk.meter());
    let got = sdk.collect();
    let ssts = &got["pigeonhole.sst.count"];
    let levels: BTreeSet<_> = ssts
        .points
        .iter()
        .map(|p| p.attrs["pigeonhole.level"].clone())
        .collect();
    assert_eq!(
        ssts.points.len(),
        levels.len(),
        "one series per level: {ssts:?}"
    );
    assert!(
        ssts.points
            .iter()
            .all(|p| !p.attrs.contains_key("pigeonhole.table"))
    );
    assert!(total(ssts) >= 6.0, "{ssts:?}");
    drop(by_level);
    // storage_by_table: one series per table, family and level.
    let sdk = Sdk::new();
    let by_table = Config::new()
        .storage_by_table(true)
        .db_name("cards")
        .register(&db, &sdk.meter());
    let got = sdk.collect();
    let ssts = &got["pigeonhole.sst.count"];
    let named: BTreeSet<_> = ssts
        .points
        .iter()
        .map(|p| p.attrs["pigeonhole.table"].clone())
        .collect();
    assert_eq!(named.len(), 6, "{ssts:?}");
    assert!(
        ssts.points
            .iter()
            .all(|p| p.attrs["pigeonhole.db.name"] == "cards")
    );
    drop(by_table);
    drop(tables);
    db.close().unwrap();
}

#[test]
fn prometheus_names_are_the_exporters() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("prom.phdb"), options()).unwrap();
    let t = table(&db, "t");
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .unwrap();
    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let metrics = pigeonhole_otel::register(&db, &provider.meter("prom"));
    workload(&db, &t);
    let mut text = String::new();
    {
        use prometheus::Encoder as _;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        text.push_str(&String::from_utf8(buf).unwrap());
    }
    for m in pigeonhole_otel::metrics() {
        if may_be_empty(&db, m.name) {
            continue;
        }
        let name = m.prometheus_name();
        assert!(
            text.lines()
                .any(|l| l.starts_with(&format!("{name}{{")) || l.starts_with(&format!("{name} "))),
            "{} isn't exported as {name}:\n{text}",
            m.name
        );
    }
    assert_eq!(
        info("pigeonhole.write_stall.time").prometheus_name(),
        "pigeonhole_write_stall_time_seconds_total"
    );
    drop(metrics);
    drop(t);
    db.close().unwrap();
}

/// The guide's metric table, as `metrics()` gives it: one row per metric, in order.
fn guide_table() -> String {
    let mut out = String::from(
        "| Metric | Prometheus name | Kind | Unit | Attributes | What it measures |\n|---|---|---|---|---|---|\n",
    );
    for m in pigeonhole_otel::metrics() {
        let kind = match m.kind {
            InstrumentKind::Counter => "counter",
            InstrumentKind::Gauge => "gauge",
            _ => unreachable!(),
        };
        let attrs = if m.attributes.is_empty() {
            "".to_owned()
        } else {
            m.attributes
                .iter()
                .map(|a| format!("`{a}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push_str(&format!(
            "| `{}` | `{}` | {kind} | `{}` | {attrs} | {} |\n",
            m.name,
            m.prometheus_name(),
            m.unit,
            m.description
        ));
    }
    out
}

/// A file of the repository's docs, with `\n` line endings: a Windows checkout may give
/// it `\r\n` (`core.autocrlf`).
fn doc_file(path: &str) -> String {
    std::fs::read_to_string(format!("{}/../../docs/{path}", env!("CARGO_MANIFEST_DIR")))
        .unwrap()
        .replace("\r\n", "\n")
}

/// The guide's table is generated from `metrics()`, so the guide, the code and this test
/// can't drift apart: on a mismatch, this prints the table to paste between the markers.
#[test]
fn the_guide_lists_every_metric_as_the_code_does() {
    let guide = doc_file("guide/observability.md");
    let (begin, end) = ("<!-- metrics:begin -->\n", "<!-- metrics:end -->");
    let want = guide_table();
    let got = guide
        .split_once(begin)
        .and_then(|(_, rest)| rest.split_once(end))
        .map(|(table, _)| table);
    assert_eq!(
        got,
        Some(want.as_str()),
        "the guide's table should be:\n{want}"
    );
}

/// Every `pigeonhole_…` name in `text` is a metric's Prometheus name or a label name; how
/// many metric names it uses.
fn known_names_only(what: &str, text: &str) -> usize {
    let known: BTreeSet<String> = pigeonhole_otel::metrics()
        .map(|m| m.prometheus_name())
        .collect();
    // Label names: the attribute keys, with dots as underscores.
    let labels: BTreeSet<String> = pigeonhole_otel::metrics()
        .flat_map(|m| m.attributes.iter().copied().chain(["pigeonhole.db.name"]))
        .map(|a| a.replace('.', "_"))
        .collect();
    let mut queried = 0;
    for (i, _) in text.match_indices("pigeonhole_") {
        if i > 0 && text[..i].ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        let name: String = text[i..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        // The crate's own path, in the guide's code.
        if labels.contains(&name) || name == "pigeonhole_otel" {
            continue;
        }
        assert!(
            known.contains(&name),
            "{what} uses {name}, which no metric is exported as"
        );
        queried += 1;
    }
    queried
}

/// Every Prometheus name the starter dashboard and the guide's alerts query is one a metric
/// is exported as.
#[test]
fn the_dashboard_and_the_alerts_query_only_documented_metrics() {
    let dashboard = doc_file("grafana/pigeonhole.json");
    assert!(known_names_only("the dashboard", &dashboard) >= 10);
    let guide = doc_file("guide/observability.md");
    assert!(known_names_only("the guide", &guide) >= 10);
}

/// A Prometheus text's families: each name's `# HELP` and `# TYPE`, and its samples' labels
/// as written (values left out, since gauges move between two readings).
type Families = BTreeMap<String, (Option<String>, Option<String>, BTreeSet<String>)>;

fn families(text: &str) -> Families {
    let mut out = Families::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').unwrap();
            out.entry(name.to_owned()).or_default().0 = Some(help.to_owned());
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').unwrap();
            out.entry(name.to_owned()).or_default().1 = Some(kind.to_owned());
        } else if !line.is_empty() {
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            let (name, labels) = match series.split_once('{') {
                Some((n, l)) => (n, l.strip_suffix('}').unwrap()),
                None => (series, ""),
            };
            // As written, label for label: ours must match the exporter's order too.
            out.entry(name.to_owned())
                .or_default()
                .2
                .insert(labels.to_owned());
        }
    }
    out
}

/// D323: `Collector::render_prometheus` writes what OpenTelemetry's Prometheus exporter
/// writes for the same database (without its scope labels and `target_info`): the same
/// families, with the same `# HELP` and `# TYPE`, and the same label sets.
#[test]
fn render_prometheus_matches_the_exporter() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("prom.phdb"), options()).unwrap();
    let t = table(&db, "t");
    let registry = prometheus::Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_target_info()
        .scope_info_enabled(false)
        .build()
        .unwrap();
    let provider = SdkMeterProvider::builder().with_reader(exporter).build();
    let registration = pigeonhole_otel::register(&db, &provider.meter("prom"));
    let collector = pigeonhole_otel::collector(&db);
    workload(&db, &t);
    let theirs = {
        use prometheus::Encoder as _;
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    };
    let mut ours = String::new();
    collector.render_prometheus(&mut ours);
    let (theirs_f, ours_f) = (families(&theirs), families(&ours));
    for (name, f) in &theirs_f {
        assert_eq!(
            ours_f.get(name),
            Some(f),
            "{name}:\nours:\n{ours}\ntheirs:\n{theirs}"
        );
    }
    assert_eq!(
        ours_f.keys().collect::<Vec<_>>(),
        theirs_f.keys().collect::<Vec<_>>(),
        "ours:\n{ours}\ntheirs:\n{theirs}"
    );
    drop(registration);
    drop(collector);
    drop(t);
    db.close().unwrap();
}

/// D323: `Collector::collect` gives each metric's points with `pigeonhole.db.name` first and
/// only the metric's own attributes after; seconds as floats, counts as integers; nothing
/// once the database is closed.
#[test]
fn a_collector_gives_plain_points_until_the_database_closes() {
    use pigeonhole_otel::PointValue;
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("c.phdb"), options()).unwrap();
    let t = table(&db, "t");
    let collector = Config::new().db_name("main").collector(&db);
    workload(&db, &t);
    let points = collector.collect();
    let sync_commits = points
        .iter()
        .find(|p| {
            p.metric.name == "pigeonhole.commits"
                && p.attributes[1] == ("pigeonhole.durability", "sync".to_owned())
        })
        .unwrap();
    assert!(matches!(sync_commits.value, PointValue::U64(n) if n >= 20));
    for p in &points {
        assert_eq!(p.attributes[0], ("pigeonhole.db.name", "main".to_owned()));
        for (k, _) in &p.attributes[1..] {
            assert!(p.metric.attributes.contains(k), "{}: {k}", p.metric.name);
        }
        if p.metric.unit == "s" {
            assert!(matches!(p.value, PointValue::F64(_)), "{}", p.metric.name);
        }
        if p.metric.unit.starts_with('{') {
            assert!(matches!(p.value, PointValue::U64(_)), "{}", p.metric.name);
        }
    }
    let levels: Vec<_> = points
        .iter()
        .filter(|p| p.metric.name == "pigeonhole.sst.count")
        .map(|p| p.attributes[1].clone())
        .collect();
    assert!(!levels.is_empty());
    assert!(
        levels
            .iter()
            .all(|(k, v)| *k == "pigeonhole.level" && v.parse::<u8>().is_ok())
    );
    drop(t);
    db.close().unwrap();
    assert!(collector.collect().is_empty());
}

/// ICR 0133: a constant attribute (`Config::attribute`) reaches every point, through the
/// OpenTelemetry registration and the collector alike, and `render_prometheus_all` writes
/// each metric's `# HELP`/`# TYPE` once for several databases, each database's samples
/// told apart by it.
#[test]
fn several_databases_render_as_one_exposition_told_apart_by_an_attribute() {
    let dir = pigeonhole::doc_support::temp_dir();
    let dbs: Vec<Pigeonhole> = ["p1", "p2"]
        .iter()
        .map(|p| Pigeonhole::open(dir.join(format!("{p}.phdb")), options()).unwrap())
        .collect();
    let tables: Vec<Table> = dbs.iter().map(|db| table(db, "t")).collect();
    for (db, t) in dbs.iter().zip(&tables) {
        workload(db, t);
    }
    let config = |p: &str| {
        Config::new()
            .db_name("same.phdb")
            .attribute("pigeonhole.partition", "replaced")
            .attribute("pigeonhole.partition", p)
    };
    // Through OpenTelemetry: every point carries it, once, with the last value set.
    let sdk = Sdk::new();
    let registration = config("p1").register(&dbs[0], &sdk.meter());
    for (name, c) in sdk.collect() {
        for p in &c.points {
            assert_eq!(
                p.attrs.get("pigeonhole.partition").map(String::as_str),
                Some("p1"),
                "{name}: {:?}",
                p.attrs
            );
        }
    }
    drop(registration);
    // Through collectors, rendered together.
    let collectors: Vec<_> = ["p1", "p2"]
        .iter()
        .zip(&dbs)
        .map(|(p, db)| config(p).collector(db))
        .collect();
    let refs: Vec<&pigeonhole_otel::Collector> = collectors.iter().collect();
    let mut text = String::new();
    pigeonhole_otel::Collector::render_prometheus_all(&refs, &mut text);
    let mut seen = BTreeSet::new();
    for line in text.lines().filter(|l| l.starts_with("# TYPE ")) {
        assert!(seen.insert(line), "{line} twice:\n{text}");
    }
    let commits = text
        .lines()
        .filter(|l| l.starts_with("pigeonhole_commits_total{"))
        .collect::<Vec<_>>();
    for p in ["p1", "p2"] {
        assert!(
            commits
                .iter()
                .any(|l| l.contains(&format!("pigeonhole_partition=\"{p}\""))),
            "{p}'s commits:\n{text}"
        );
    }
    // A collected point carries it right after `pigeonhole.db.name` (ai1249's N2).
    for p in refs[1].collect() {
        assert_eq!(
            p.attributes.get(1).map(|(k, v)| (*k, v.as_str())),
            Some(("pigeonhole.partition", "p2")),
            "{}: {:?}",
            p.metric.name,
            p.attributes
        );
    }
    // One database alone renders as `render_prometheus` does: the same lines, values aside
    // (gauges may move between two readings).
    let (mut one, mut all) = (String::new(), String::new());
    refs[0].render_prometheus(&mut one);
    pigeonhole_otel::Collector::render_prometheus_all(&refs[..1], &mut all);
    let unvalued = |t: &str| -> Vec<String> {
        t.lines()
            .map(|l| match l.starts_with('#') {
                true => l.to_owned(),
                false => l.rsplit_once(' ').map_or(l, |(k, _)| k).to_owned(),
            })
            .collect()
    };
    assert_eq!(unvalued(&one), unvalued(&all));
    drop(collectors);
    drop(tables);
    for db in dbs {
        db.close().unwrap();
    }
}

/// ai1249's S1 on #1711: a `Config::attribute` key becomes a valid Prometheus label name, as
/// the exporter writes one, whatever it holds.
#[test]
fn an_attribute_key_renders_as_a_valid_label_name() {
    let dir = pigeonhole::doc_support::temp_dir();
    let db = Pigeonhole::open(dir.join("labels.phdb"), options()).unwrap();
    let t = table(&db, "t");
    workload(&db, &t);
    let c = Config::new()
        .attribute("service-tier/k8s.pod", "a")
        .attribute("2nd.zone", "b")
        .collector(&db);
    let mut text = String::new();
    c.render_prometheus(&mut text);
    let commits = text
        .lines()
        .find(|l| l.starts_with("pigeonhole_commits_total{"))
        .expect("a commits sample");
    assert!(commits.contains("service_tier_k8s_pod=\"a\""), "{commits}");
    assert!(commits.contains("_2nd_zone=\"b\""), "{commits}");
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let labels = line.split_once('{').map_or("", |(_, rest)| rest);
        for name in labels
            .split(',')
            .filter_map(|kv| kv.split_once('=').map(|(k, _)| k))
        {
            assert!(
                !name.starts_with(|c: char| c.is_ascii_digit())
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "label {name:?} in {line}"
            );
        }
    }
    drop(c);
    drop(t);
    db.close().unwrap();
}

/// ai1249's N1 on #1711: a key every point or some metric already carries is refused at
/// once, rather than writing a duplicate label Prometheus refuses.
#[test]
#[should_panic(expected = "is an attribute the metrics carry already")]
fn an_attribute_the_metrics_carry_is_refused() {
    let _ = Config::new().attribute("pigeonhole.db.name", "x");
}

#[test]
#[should_panic(expected = "is an attribute the metrics carry already")]
fn a_metrics_own_attribute_is_refused() {
    let _ = Config::new().attribute("pigeonhole.table", "x");
}

/// Keys are compared as label names (ai1249's follow-up on #1711): one that renders as an
/// attribute the metrics carry, or as another key set, is refused; the same key again only
/// replaces its value.
#[test]
#[should_panic(expected = "is an attribute the metrics carry already")]
fn a_key_that_renders_as_db_name_is_refused() {
    let _ = Config::new().attribute("pigeonhole_db_name", "x");
}

#[test]
#[should_panic(expected = "has the label name \"a_b\" of another attribute set")]
fn two_keys_with_one_label_name_are_refused() {
    let _ = Config::new().attribute("a.b", "x").attribute("a-b", "y");
}

#[test]
fn the_same_key_again_replaces_its_value() {
    let _ = Config::new().attribute("a.b", "x").attribute("a.b", "y");
}
