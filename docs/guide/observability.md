# Observability

> **Status:** this guide describes 1.2. `pigeonhole-otel` is new in 1.2.0 (D316, #1458, ICR 0115).

`pigeonhole-otel` exports a database's engine metrics through [OpenTelemetry](https://opentelemetry.io), so they reach Prometheus and Grafana, or any OTLP backend, beside your application's own metrics. One call registers them with your `Meter`, and there's no polling loop to write.

```toml
[dependencies]
pigeonhole-otel = "0.1"
opentelemetry = "0.33"
opentelemetry_sdk = "0.33"
```

`pigeonhole-otel` has its own 0.N version line, outside `pigeonhole`'s 1.x line, because its API takes `opentelemetry` types: each `opentelemetry` minor release is a new `pigeonhole-otel` minor. Use the `opentelemetry` minor it names. **The metric names are a stable contract of their own** (D316): a rename is a breaking change, whatever the crate's version.

## Prometheus
With `opentelemetry-prometheus`, the metrics land in a `prometheus::Registry`, which your HTTP server serves at `/metrics`:

```rust
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use pigeonhole::{Options, Pigeonhole};
use prometheus::{Encoder as _, TextEncoder};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;

let registry = prometheus::Registry::new();
let exporter = opentelemetry_prometheus::exporter()
    .with_registry(registry.clone())
    .build()?;
let provider = SdkMeterProvider::builder().with_reader(exporter).build();

// Keep the registration for as long as you want the metrics: dropping it stops them.
let _metrics = pigeonhole_otel::register(&db, &provider.meter("my-app"));

// What a scrape of /metrics returns:
let mut body = Vec::new();
TextEncoder::new().encode(&registry.gather(), &mut body)?;
assert!(String::from_utf8(body)?.contains("pigeonhole_commits_total"));
# drop(_metrics);
# db.close()?;
# Ok(())
# }
```

## An OTLP collector
To push to an OpenTelemetry collector, or to any OTLP backend, use `opentelemetry-otlp`'s metric exporter behind a `PeriodicReader`. Register the same way. This needs `opentelemetry-otlp = { version = "0.33", features = ["metrics", "grpc-tonic"] }` beside `opentelemetry_sdk`, and a Tokio runtime for tonic's gRPC client:

```rust,ignore
let exporter = opentelemetry_otlp::MetricExporter::builder()
    .with_tonic()
    .with_endpoint("http://localhost:4317")
    .build()?;
let provider = SdkMeterProvider::builder()
    .with_reader(PeriodicReader::builder(exporter).with_interval(Duration::from_secs(15)).build())
    .with_resource(Resource::builder().with_service_name("my-app").build())
    .build();
let _metrics = pigeonhole_otel::register(&db, &provider.meter("my-app"));
```

The collector's `prometheus` or `prometheusremotewrite` exporter then gives the same names as the Prometheus exporter above.

## Without an OpenTelemetry SDK
If your server serves its own `/metrics`, or your backend isn't OpenTelemetry, a `Collector` reads the same metrics without a `Meter`. `collect()` gives them as plain points, and `render_prometheus` writes them as Prometheus text, under the names the Prometheus exporter above gives (without its `otel_scope_*` labels and `target_info`):

```rust
use pigeonhole::{Options, Pigeonhole};

# fn main() -> pigeonhole::Result<()> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
let metrics = pigeonhole_otel::collector(&db);

// In your /metrics handler:
let mut text = String::new();
metrics.render_prometheus(&mut text);
assert!(text.contains("pigeonhole_commits_total{pigeonhole_db_name=\"app.phdb\""));

// Or the points themselves:
for point in metrics.collect() {
    let _ = (point.metric.name, point.value, &point.attributes);
}
# drop(metrics);
# db.close()?;
# Ok(())
# }
```

A collection reads the database on the calling thread, as an exporter's collection does (see [What it costs](#what-it-costs)). `Config::new()…collector(&db)` takes the same options as `register`, and a collector has `rebind` and `watch_feed` too.

For several databases in one `/metrics`, render them together with `Collector::render_prometheus_all(&[&a, &b], &mut text)`. The text format allows each metric's `# HELP` and `# TYPE` once, so rendering the collectors one after another would repeat them.

**`phdb serve`** does this: its `/metrics` carries every metric below for the database it serves, after the server's own (see the [CLI guide](cli.md)).

## Options
`pigeonhole_otel::Config` sets four things:
- **`db_name`:** the `pigeonhole.db.name` attribute every metric carries. By default it's the database file's name (`app.phdb`). It tells the databases of one process apart. The OpenTelemetry **resource** (`service.name`, `host.name`, `service.instance.id`) tells processes apart, so the same file name on many hosts doesn't collide.
- **`storage_by_table(true)`:** the SST and blob gauges also carry `pigeonhole.table` and `pigeonhole.family`. Off by default; see [Cardinality](#cardinality).
- **`latency_window`:** the window `pigeonhole.commit.engine_latency` covers, 60 seconds by default.
- **`attribute(key, value)`:** a constant attribute every metric carries, after `pigeonhole.db.name`, such as `phdb serve`'s `pigeonhole.partition` on a cluster node. Use a key no metric carries itself, and a bounded set of values: each value is another series per metric.

```rust,ignore
let _metrics = pigeonhole_otel::Config::new()
    .db_name("orders")
    .latency_window(Duration::from_secs(30))
    .register(&db, &meter);
```

**Watching a change feed's lag.** A feed's subscriber is the application's own, so it names the feeds it wants watched:

```rust,ignore
let mut feed = orders.feed().open()?;
metrics.watch_feed("indexer", feed.progress());
// The indexer's thread reads `feed` as usual; the exporter reads its position.
```

`pigeonhole.feed.lag{pigeonhole.feed.name="indexer"}` is then the seqnos between the database's visible seqno and the feed's covered position: about one per commit (the unit is `{commit}`), though a cross-shard round that aborts also uses one. It stays near 0 while the subscriber keeps up, even when the writes are to tables the feed doesn't carry, because the feed's position moves past those too. A durable feed (the default) delivers a commit once it's durable, so it also trails by any `Buffered` or `None` commits not yet synced, up to the feed's sync interval. A feed that failed or expired but is still held stops moving, so its lag grows until the application drops it, which is what to alert on. Watch only named, long-lived subscribers: each name is a series. A dropped feed stops reporting.

**Register in the writer process,** once per database. The writer's metrics already include what operators need about reader processes (`pigeonhole.reader_slots.*`, `pigeonhole.reader.seqno_lag`).

**Keep the `Registration`.** It is `#[must_use]`: a bare `register(..);` drops it at once and reports nothing. Bind it to a named variable (`let _metrics = …`), not `_`. Dropping it stops reporting and releases its handle to the database. After a reopen, `registration.rebind(&new_db)` points it at the new handle and keeps every counter growing from where it was.

## What it costs
Every instrument is observable, so nothing runs on any engine thread. Commits and reads pay nothing. At each collection, the first instrument's callback reads the database once, on the exporter's thread, and the other instruments reuse that reading:
- `engine_metrics()`: relaxed atomic loads, about 300 a shard;
- `recovery_stats()`;
- `info()`: a walk of the catalog's SST list, with no I/O.

That's microseconds per collection. At a 15-second interval it's nothing.

## The metrics
Every metric carries `pigeonhole.db.name`, and the attributes listed below. "Prometheus name" is what OpenTelemetry's Prometheus exporter calls it, as `opentelemetry-prometheus` 0.33 translates names by default (a test checks against 0.33's real output): dots become underscores, and the unit's word (`_seconds`, `_bytes`, `_ratio`) is appended. A counter also gains `_total`. A test checks each name against the exporter's real output.

Counters are cumulative from registration. They stay monotonic across an automatic recovery (`Options::auto_recover`), whose new engine's own counters restart from 0, and across a `rebind`. Metrics a database doesn't have report no values: the change feed's while it's off, and io_uring's on other backends.

<!-- metrics:begin -->
| Metric | Prometheus name | Kind | Unit | Attributes | What it measures |
|---|---|---|---|---|---|
| `pigeonhole.commits` | `pigeonhole_commits_total` | counter | `{commit}` | `pigeonhole.durability` | Commits completed, by durability level. |
| `pigeonhole.commit.engine_latency` | `pigeonhole_commit_engine_latency_seconds` | gauge | `s` | `pigeonhole.durability`, `pigeonhole.quantile` | Engine-side commit latency over the latency window: from a commit's submission to its resolution on its shard, excluding the caller's wake-up and the visibility wait. The upper bound of the quantile's bucket; reported for levels with commits in the window. |
| `pigeonhole.group_syncs` | `pigeonhole_group_syncs_total` | counter | `{sync}` |  | Group syncs finished: each is one WAL sync covering the group_sync commits batched behind it, or a buffered group whose record waits for a WAL rollover's sync (at most one per rollover). |
| `pigeonhole.wal.group_sync.time` | `pigeonhole_wal_group_sync_time_seconds_total` | counter | `s` |  | Time group syncs were in flight, summed over shards: over pigeonhole.group_syncs, the mean group sync time. Group syncs only (group_sync commits, or buffered ones held behind a WAL rollover's sync): not a sync commit's own sync, or flush or checkpoint barrier syncs. |
| `pigeonhole.transactions` | `pigeonhole_transactions_total` | counter | `{transaction}` | `pigeonhole.outcome` | Transactions committed (outcome committed) and refused with Conflict (outcome conflict). |
| `pigeonhole.transaction.retries` | `pigeonhole_transaction_retries_total` | counter | `{retry}` |  | Attempts Pigeonhole::transact retried. |
| `pigeonhole.cross_shard.rounds` | `pigeonhole_cross_shard_rounds_total` | counter | `{round}` |  | Two-phase rounds of cross-shard commits decided. |
| `pigeonhole.cross_shard.failed_fast` | `pigeonhole_cross_shard_failed_fast_total` | counter | `{commit}` |  | Cross-shard commits that failed with Conflict without another round. |
| `pigeonhole.write_stalls` | `pigeonhole_write_stalls_total` | counter | `{stall}` | `pigeonhole.stall.reason` | Write stalls: commits held for memtable arena room (reason room) or paced because L0 was too deep (reason pacing). |
| `pigeonhole.write_stall.time` | `pigeonhole_write_stall_time_seconds_total` | counter | `s` | `pigeonhole.stall.reason` | Time the shards spent in write stalls, summed over shards; its rate is the fraction of time stalled. |
| `pigeonhole.flushes` | `pigeonhole_flushes_total` | counter | `{flush}` | `pigeonhole.outcome` | Memtable flushes completed (outcome success) and failed (outcome failure; retried, nothing lost). |
| `pigeonhole.compactions` | `pigeonhole_compactions_total` | counter | `{compaction}` | `pigeonhole.outcome` | Compactions completed (success), failed or unable to start (failure), and taken back from an application's worker (reclaimed). |
| `pigeonhole.background.take_backs` | `pigeonhole_background_take_backs_total` | counter | `{task}` | `pigeonhole.take_back.reason` | Background tasks the shards took back from application-owned workers: not driven for 10 ms (reason undriven), or held while a commit waited for room (reason room). Nonzero means the workers aren't driven. |
| `pigeonhole.shard.busy.time` | `pigeonhole_shard_busy_time_seconds_total` | counter | `s` |  | Time the engine-owned shard threads spent working, summed over shards; its rate is how many shards' worth of work the database did. |
| `pigeonhole.memtable.size` | `pigeonhole_memtable_size_bytes` | gauge | `By` |  | Memtable arena bytes in use, summed over shards: live memtables in whole chunks, and flushed ones a snapshot or read still holds. Near the limit, commits wait for room. |
| `pigeonhole.memtable.limit` | `pigeonhole_memtable_limit_bytes` | gauge | `By` |  | The memtable arenas' size, summed over shards: Options::memtable_budget (per shard, rounded up to 2 MiB) times the shard count. |
| `pigeonhole.block_cache.requests` | `pigeonhole_block_cache_requests_total` | counter | `{request}` | `pigeonhole.cache.result` | Block cache lookups that found the block (hit) or read it from the file (miss). |
| `pigeonhole.block_cache.capacity` | `pigeonhole_block_cache_capacity_bytes` | gauge | `By` |  | The block cache's capacity. |
| `pigeonhole.block_cache.pinned` | `pigeonhole_block_cache_pinned_bytes` | gauge | `By` |  | SST indexes and filters pinned in the block cache; past half the capacity, reads start costing two reads. |
| `pigeonhole.block_cache.pinned_warnings` | `pigeonhole_block_cache_pinned_warnings_total` | counter | `{warning}` |  | Times the pinned bytes rose past half the block cache's capacity. |
| `pigeonhole.row_cache.requests` | `pigeonhole_row_cache_requests_total` | counter | `{request}` | `pigeonhole.cache.result` | Row cache lookups that found the row (hit) or not (miss). |
| `pigeonhole.row_cache.fills` | `pigeonhole_row_cache_fills_total` | counter | `{fill}` | `pigeonhole.fill.source` | Rows cached after a read (source read) or written through by a commit (source write). |
| `pigeonhole.file.size` | `pigeonhole_file_size_bytes` | gauge | `By` |  | The database file's length. |
| `pigeonhole.file.free` | `pigeonhole_file_free_bytes` | gauge | `By` |  | Bytes of the file free for reuse. |
| `pigeonhole.file.growths` | `pigeonhole_file_growths_total` | counter | `{growth}` |  | Times the file grew because the page allocator ran out of free space. |
| `pigeonhole.file.growth.time` | `pigeonhole_file_growth_time_seconds_total` | counter | `s` |  | Time those growths held the page allocator. |
| `pigeonhole.sst.count` | `pigeonhole_sst_count` | gauge | `{sst}` | `pigeonhole.level`, `pigeonhole.table`, `pigeonhole.family` | SSTs per LSM level; level 0's count is the compaction-debt signal. Table and family only with Config::storage_by_table. |
| `pigeonhole.sst.size` | `pigeonhole_sst_size_bytes` | gauge | `By` | `pigeonhole.level`, `pigeonhole.table`, `pigeonhole.family` | SST bytes per LSM level. Table and family only with Config::storage_by_table. |
| `pigeonhole.blob.size` | `pigeonhole_blob_size_bytes` | gauge | `By` | `pigeonhole.blob.state`, `pigeonhole.table`, `pigeonhole.family` | Blob file bytes (state total) and the live bytes in them (state live); the gap is what blob GC can reclaim. |
| `pigeonhole.reader_slots.used` | `pigeonhole_reader_slots_used` | gauge | `{slot}` |  | Reader processes attached. |
| `pigeonhole.reader_slots.limit` | `pigeonhole_reader_slots_limit` | gauge | `{slot}` |  | The reader slot count. |
| `pigeonhole.reader_slots.reclaimed` | `pigeonhole_reader_slots_reclaimed_total` | counter | `{slot}` |  | Reader slots freed because their process died without closing. |
| `pigeonhole.reader.seqno_lag` | `pigeonhole_reader_seqno_lag` | gauge | `{commit}` |  | How many commits behind the newest the oldest reader snapshot is; a growing value holds back reclamation. |
| `pigeonhole.threads` | `pigeonhole_threads` | gauge | `{thread}` | `pigeonhole.thread.pinned` | Threads the engine started, pinned to a CPU of their own or not. |
| `pigeonhole.async.sync_reads` | `pigeonhole_async_sync_reads_total` | counter | `{read}` |  | File reads made synchronously inside async reads, blocking their executor thread. |
| `pigeonhole.io.rings` | `pigeonhole_io_rings` | gauge | `{ring}` |  | io_uring rings the database has (io_uring only). |
| `pigeonhole.io.ring_attach_failures` | `pigeonhole_io_ring_attach_failures_total` | counter | `{failure}` |  | Times a thread could not get an io_uring ring (io_uring only). |
| `pigeonhole.io.inline` | `pigeonhole_io_inline_total` | counter | `{op}` |  | I/O run inline on the calling thread instead of through io_uring (io_uring only). |
| `pigeonhole.feed.syncs` | `pigeonhole_feed_syncs_total` | counter | `{sync}` | `pigeonhole.feed.sync.kind` | Syncs the change feed made on its own (kind background) and of its sidecar file (kind sidecar); only while the feed is on. |
| `pigeonhole.feed.retained.size` | `pigeonhole_feed_retained_size_bytes` | gauge | `By` |  | WAL bytes the change feed retains. |
| `pigeonhole.feed.retained.limit` | `pigeonhole_feed_retained_limit_bytes` | gauge | `By` |  | The change feed's retention limit; near it, slow subscribers start expiring. |
| `pigeonhole.feed.held_blobs.size` | `pigeonhole_feed_held_blobs_size_bytes` | gauge | `By` |  | Blob bytes the change feed's retained history holds. |
| `pigeonhole.feed.lag` | `pigeonhole_feed_lag` | gauge | `{commit}` | `pigeonhole.feed.name` | Seqnos between the database's visible seqno and a watched feed's covered position (Registration::watch_feed): about one a commit, so near 0 while the feed keeps up and growing while its subscriber falls behind or after it failed. |
| `pigeonhole.failed` | `pigeonhole_failed_ratio` | gauge | `1` |  | 0 or 1: 1 once the instance has failed (Pigeonhole::is_failed). The first alert to set. |
| `pigeonhole.recoveries` | `pigeonhole_recoveries_total` | counter | `{recovery}` |  | Automatic recoveries completed. |
| `pigeonhole.recovery.gave_up` | `pigeonhole_recovery_gave_up_ratio` | gauge | `1` |  | 0 or 1: 1 once automatic recovery gave up. |
<!-- metrics:end -->

**`pigeonhole.commit.engine_latency` is engine-side latency.** It runs from a commit's submission to its resolution on its shard, and excludes the caller's wake-up, the wait for the commit to become visible, and the return. Those are often where a commit's tail is: in one measured case, the engine-side p99.9 was 98 µs while the client saw 391 µs (#1206). For end-to-end latency, use your application's own spans.
- **Resolution:** each value is the upper bound of the bucket its quantile falls in, so it errs high. Adjacent buckets differ by about 1.41×.
- **Coverage:** the quantiles cover the commits of the last `latency_window`, whatever the collection interval, and are reported only for durability levels with commits in that window.
- **Aggregation:** quantiles can't be aggregated across databases or instances (an average of p99s isn't a p99).

## What to alert on
In Prometheus terms:

| Alert | Query | Why |
|---|---|---|
| The database failed | `pigeonhole_failed_ratio == 1` | A failed instance refuses writes until it's reopened or recovered. Set this one first. |
| Recovery gave up | `pigeonhole_recovery_gave_up_ratio == 1` | Automatic recovery stopped retrying. |
| Writes are stalling | `sum by (pigeonhole_db_name) (rate(pigeonhole_write_stall_time_seconds_total[5m])) > 0.05` | Commits held more than 5% of the time. `pigeonhole_stall_reason="pacing"` means compaction is behind; `"room"` means the memtable arena is too small, or snapshots hold flushed memtables. |
| The memtable is nearly full | `pigeonhole_memtable_size_bytes > 0.9 * pigeonhole_memtable_limit_bytes` | An early, approximate warning: commits can also wait for room below it (reserved room, a cross-shard share awaiting its decision, a value larger than the longest free run). The ground truth is the stall rate with `pigeonhole_stall_reason="room"`. Raise `Options::memtable_budget` (per shard), or look for snapshots holding flushed memtables. |
| Compaction is behind | `pigeonhole_sst_count{pigeonhole_level="0"} > 8` | L0's depth is what paces commits. |
| The block cache is too small | `pigeonhole_block_cache_pinned_bytes > 0.5 * pigeonhole_block_cache_capacity_bytes` | Past half the capacity, a cold read costs two reads rather than one. |
| The feed is near its retention | `pigeonhole_feed_retained_size_bytes > 0.9 * pigeonhole_feed_retained_limit_bytes` | Slow subscribers start expiring with `FeedExpired`. |
| An old reader snapshot | `pigeonhole_reader_seqno_lag > 1e6` | It holds back reclamation of the versions it can see. |
| Commit latency | `pigeonhole_commit_engine_latency_seconds{pigeonhole_quantile="0.99"} > 0.01` | Engine-side p99 over the latency window. Pick the threshold from your own baseline. |

The mean group sync time is `rate(pigeonhole_wal_group_sync_time_seconds_total[5m]) / rate(pigeonhole_group_syncs_total[5m])`, the WAL device's sync latency as the commits see it.

The block cache's hit ratio is `rate(pigeonhole_block_cache_requests_total{pigeonhole_cache_result="hit"}[5m]) / rate(pigeonhole_block_cache_requests_total[5m])`.

## Cardinality
With the defaults, the number of series is fixed. It doesn't grow with the data or the schema: four durability levels, three quantiles, a handful of outcomes and reasons, and one series per LSM level. Nothing is ever keyed by a row, key, qualifier or sequence number.

`storage_by_table(true)` multiplies the SST and blob gauges by the number of table-family pairs. That's bounded by the schema, so turn it on only for a schema with a modest, fixed set of tables, never for one that creates tables per user or tenant.

## A Grafana dashboard
[`docs/grafana/pigeonhole.json`](../grafana/pigeonhole.json) is a starter dashboard over the Prometheus names above:
- commits by durability;
- engine-side latency;
- the stall fraction;
- L0 depth;
- the cache hit ratios;
- file size;
- the feed's retention;
- health.

Import it into Grafana and pick your Prometheus data source. A test checks that every name it queries is one a metric is exported as.

## Python and Ruby
In 1.2.0 the metrics are Rust-only: neither binding exposes `engine_metrics` or `pigeonhole-otel` yet. Binding access is a follow-up.
