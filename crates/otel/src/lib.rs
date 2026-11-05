//! Exports a [Pigeonhole](pigeonhole) database's engine metrics through OpenTelemetry
//! (decision D316, #1458; API in ICR 0115), so they reach Prometheus, Grafana or any OTLP
//! backend beside the application's own metrics.
//!
//! [`register`] adds one observable instrument per metric to an application's
//! [`Meter`]. Nothing runs until the application's exporter
//! collects: then the first instrument's callback reads the database once
//! ([`Pigeonhole::engine_metrics`], `recovery_stats`, `info`, on the exporter's thread), and
//! every other instrument reuses that reading. Commits and reads pay nothing.
//!
//! ```
//! use opentelemetry::metrics::MeterProvider as _;
//! use pigeonhole::{Options, Pigeonhole};
//!
//! # fn main() -> pigeonhole::Result<()> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(1))?;
//! # let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
//! let meter = provider.meter("my-app");
//! // Keep the registration: dropping it stops reporting (so not `let _ = …`).
//! let _metrics = pigeonhole_otel::register(&db, &meter);
//! # drop(_metrics);
//! # db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! **The metrics** are listed by [`metrics`], and in the
//! [observability guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/observability.md)
//! with what to alert on. Their names, kinds, units and attributes are a stable contract
//! (D316): a rename is a breaking change, whatever this crate's version.
//!
//! **Versions.** This crate has its own 0.N version line, outside `pigeonhole`'s 1.x line: its
//! API takes `opentelemetry` types, so each `opentelemetry` minor release is a new minor here.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{AsyncInstrument, Meter};
use pigeonhole::{DatabaseInfo, Durability, EngineMetrics, IoRings, LatencyHistogram, Pigeonhole};

/// The user guide's observability page and the crate README, compiled and run as doctests.
#[cfg(doctest)]
mod guide {
    #[doc = include_str!("../../../docs/guide/observability.md")]
    struct Observability;
    #[doc = include_str!("../README.md")]
    struct CrateReadme;
}

/// Registers `db`'s metrics with `meter`, with the default [`Config`]: the database is named
/// by its file name, storage is reported per level, and commit latency over the last 60
/// seconds.
///
/// Register once per database, in the writer process. Dropping the [`Registration`] stops
/// reporting.
#[must_use = "dropping the Registration stops reporting"]
pub fn register(db: &Pigeonhole, meter: &Meter) -> Registration {
    Config::default().register(db, meter)
}

/// How [`Config::register`] reports a database.
#[derive(Debug, Clone)]
pub struct Config {
    db_name: Option<String>,
    storage_by_table: bool,
    latency_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_name: None,
            storage_by_table: false,
            latency_window: Duration::from_secs(60),
        }
    }
}

impl Config {
    /// The defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `pigeonhole.db.name` attribute every metric carries. By default it is the
    /// database file's name ([`Pigeonhole::path`]'s last component, `app.phdb`), or the whole
    /// path when it has none.
    ///
    /// It tells the databases of one process apart. The OpenTelemetry *resource*
    /// (`service.name`, `host.name`, `service.instance.id`) tells processes apart, so the same
    /// file name on many hosts does not collide.
    pub fn db_name(mut self, name: impl Into<String>) -> Self {
        self.db_name = Some(name.into());
        self
    }

    /// Whether the SST and blob gauges also carry `pigeonhole.table` and
    /// `pigeonhole.family` (default `false`: one series per level, summed over the tables).
    ///
    /// **Cardinality:** on, those gauges get one series per table, family and level, so their
    /// series grow with the schema. Turn it on only for a schema of a bounded, modest number
    /// of tables; never for one that creates tables per user or per tenant.
    pub fn storage_by_table(mut self, on: bool) -> Self {
        self.storage_by_table = on;
        self
    }

    /// The window `pigeonhole.commit.engine_latency`'s quantiles cover: the commits of the
    /// last `window` (default 60 seconds), whatever the exporter's collection interval.
    /// Values below one second count as one second.
    pub fn latency_window(mut self, window: Duration) -> Self {
        self.latency_window = window.max(Duration::from_secs(1));
        self
    }

    /// Registers `db`'s metrics with `meter` (see [`register`]).
    #[must_use = "dropping the Registration stops reporting"]
    pub fn register(self, db: &Pigeonhole, meter: &Meter) -> Registration {
        let name = self.db_name.clone().unwrap_or_else(|| default_name(db));
        let shared = Arc::new(Shared {
            name: KeyValue::new(attr::DB_NAME, name),
            storage_by_table: self.storage_by_table,
            window: self.latency_window,
            state: Mutex::new(State::new(db)),
        });
        let instruments = SPECS
            .iter()
            .map(|spec| build(spec, meter, &shared))
            .collect();
        Registration {
            shared,
            _instruments: instruments,
        }
    }
}

fn default_name(db: &Pigeonhole) -> String {
    let path = db.path();
    match path.file_name() {
        Some(f) => f.to_string_lossy().into_owned(),
        None => path.display().to_string(),
    }
}

/// A database's metrics, registered with a `Meter` ([`register`]). Dropping it stops
/// reporting: the instruments' callbacks then observe nothing, and the registration's handle
/// to the database is released, so it doesn't keep the database open.
///
/// OpenTelemetry has no way to remove an observable instrument from its `Meter`, so the
/// instruments stay registered (and listed by the exporter, without values) until the
/// meter provider is dropped.
#[must_use = "dropping the Registration stops reporting"]
pub struct Registration {
    shared: Arc<Shared>,
    _instruments: Vec<Box<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for Registration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registration")
            .field("db_name", &self.shared.name.value)
            .finish_non_exhaustive()
    }
}

impl Registration {
    /// Points the registration at `db`, a reopen of the database, keeping every counter
    /// monotonic: each continues from the value it reached, rather than restarting from 0.
    /// The `pigeonhole.db.name` attribute stays the one the registration was made with, even
    /// when `db`'s file name differs, so the series continue; the other settings stay too.
    pub fn rebind(&self, db: &Pigeonhole) {
        self.shared.lock().rebind(db);
    }

    /// Reports `progress`'s feed as `pigeonhole.feed.lag{pigeonhole.feed.name=name}`: the
    /// commits between the database's visible seqno and the feed's position (D316 A4). A
    /// feed that keeps up reads near 0, even when the database's writes are to tables the
    /// feed doesn't carry. Watching another feed under the same name replaces the first.
    /// It stops once the feed is dropped.
    ///
    /// The feeds watched are the names given here, so they're the series' bound: name each
    /// subscriber of the application, never a per-request or per-user feed.
    pub fn watch_feed(&self, name: impl Into<String>, progress: pigeonhole::FeedProgress) {
        let name = name.into();
        let mut state = self.shared.lock();
        state.feeds.retain(|(n, _)| *n != name);
        state.feeds.push((name, progress));
        state.cached = None;
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        state.db = None;
        state.cached = None;
    }
}

/// Attribute keys (D316).
mod attr {
    pub const DB_NAME: &str = "pigeonhole.db.name";
    pub const DURABILITY: &str = "pigeonhole.durability";
    pub const OUTCOME: &str = "pigeonhole.outcome";
    pub const STALL_REASON: &str = "pigeonhole.stall.reason";
    pub const CACHE_RESULT: &str = "pigeonhole.cache.result";
    pub const LEVEL: &str = "pigeonhole.level";
    pub const TABLE: &str = "pigeonhole.table";
    pub const FAMILY: &str = "pigeonhole.family";
    pub const QUANTILE: &str = "pigeonhole.quantile";
    pub const TAKE_BACK_REASON: &str = "pigeonhole.take_back.reason";
    pub const FILL_SOURCE: &str = "pigeonhole.fill.source";
    pub const BLOB_STATE: &str = "pigeonhole.blob.state";
    pub const THREAD_PINNED: &str = "pigeonhole.thread.pinned";
    pub const FEED_SYNC_KIND: &str = "pigeonhole.feed.sync.kind";
    pub const FEED_NAME: &str = "pigeonhole.feed.name";
}

/// An instrument's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InstrumentKind {
    /// A monotonic sum (an observable counter): a cumulative count or time.
    Counter,
    /// A value now (an observable gauge).
    Gauge,
}

/// One metric of the contract ([`metrics`], D316).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MetricInfo {
    /// Its OpenTelemetry name, `pigeonhole.…`.
    pub name: &'static str,
    /// Counter or gauge.
    pub kind: InstrumentKind,
    /// Its UCUM unit: `s`, `By`, `1`, or a `{…}` annotation for a count.
    pub unit: &'static str,
    /// The attributes it carries besides `pigeonhole.db.name`, which every metric carries.
    pub attributes: &'static [&'static str],
    /// What it measures.
    pub description: &'static str,
}

impl MetricInfo {
    /// The name OpenTelemetry's Prometheus exporter gives it, as `opentelemetry-prometheus`
    /// 0.33 (the version this crate's tests check it against) translates names by default: dots become underscores, the unit's word is appended
    /// (`_seconds`, `_bytes`, `_ratio`; a `{…}` annotation adds nothing), and a counter gains
    /// `_total`.
    pub fn prometheus_name(&self) -> String {
        let mut name = self.name.replace('.', "_");
        match self.unit {
            "s" => name.push_str("_seconds"),
            "By" => name.push_str("_bytes"),
            "1" => name.push_str("_ratio"),
            _ => {}
        }
        if self.kind == InstrumentKind::Counter {
            name.push_str("_total");
        }
        name
    }
}

/// Every metric [`register`] reports (D316), in the guide's order. Gauges and counters a
/// database doesn't have (the change feed's while it is off, io_uring's on other backends)
/// report no values.
pub fn metrics() -> impl ExactSizeIterator<Item = MetricInfo> {
    SPECS.iter().map(|s| s.info)
}

/// A metric's value in one reading.
#[derive(Clone, Copy)]
enum Value {
    U(u64),
    F(f64),
}

/// One reading's values of a metric, each with its attributes (`pigeonhole.db.name` aside).
type Points = Vec<(Value, Vec<KeyValue>)>;

/// One metric: its contract and how it reads a snapshot.
struct Spec {
    info: MetricInfo,
    /// `F64` instruments carry seconds or quantiles, `U64` everything else.
    float: bool,
    read: fn(&Snapshot, &mut Points),
}

macro_rules! spec {
    ($name:literal, $kind:ident, $float:literal, $unit:literal, [$($attr:expr),*], $desc:literal, $read:expr) => {
        Spec {
            info: MetricInfo {
                name: $name,
                kind: InstrumentKind::$kind,
                unit: $unit,
                attributes: &[$($attr),*],
                description: $desc,
            },
            float: $float,
            read: $read,
        }
    };
}

fn one(out: &mut Points, v: u64) {
    out.push((Value::U(v), Vec::new()));
}

fn kv(key: &'static str, value: &'static str) -> Vec<KeyValue> {
    vec![KeyValue::new(key, value)]
}

fn seconds(nanos: u64) -> Value {
    Value::F(nanos as f64 / 1e9)
}

const DURABILITIES: [(Durability, &str); 4] = [
    (Durability::None, "none"),
    (Durability::Buffered, "buffered"),
    (Durability::GroupSync, "group_sync"),
    (Durability::Sync, "sync"),
];

const QUANTILES: [(f64, &str); 3] = [(0.5, "0.5"), (0.99, "0.99"), (0.999, "0.999")];

static SPECS: &[Spec] = &[
    // Commits and transactions.
    spec!(
        "pigeonhole.commits",
        Counter,
        false,
        "{commit}",
        [attr::DURABILITY],
        "Commits completed, by durability level.",
        |s, out| for (i, (_, d)) in DURABILITIES.iter().enumerate() {
            out.push((Value::U(s.e.commits[i]), kv(attr::DURABILITY, d)));
        }
    ),
    spec!(
        "pigeonhole.commit.engine_latency",
        Gauge,
        true,
        "s",
        [attr::DURABILITY, attr::QUANTILE],
        "Engine-side commit latency over the latency window: from a commit's submission to its resolution on its shard, excluding the caller's wake-up and the visibility wait. The upper bound of the quantile's bucket; reported for levels with commits in the window.",
        |s, out| for (i, (_, d)) in DURABILITIES.iter().enumerate() {
            for (j, (_, q)) in QUANTILES.iter().enumerate() {
                if let Some(v) = s.latency[i][j] {
                    out.push((
                        Value::F(v),
                        vec![
                            KeyValue::new(attr::DURABILITY, *d),
                            KeyValue::new(attr::QUANTILE, *q),
                        ],
                    ));
                }
            }
        }
    ),
    spec!(
        "pigeonhole.group_syncs",
        Counter,
        false,
        "{sync}",
        [],
        "Group syncs finished: each is one WAL sync covering the group_sync commits batched behind it.",
        |s, out| one(out, s.e.group_syncs)
    ),
    spec!(
        "pigeonhole.wal.group_sync.time",
        Counter,
        true,
        "s",
        [],
        "Time group syncs were in flight, summed over shards: over pigeonhole.group_syncs, the mean group sync time. Group syncs only (group_sync commits): not a sync commit's own sync, flush or checkpoint barrier syncs, or buffered commits.",
        |s, out| out.push((seconds(s.e.group_sync_nanos), Vec::new()))
    ),
    spec!(
        "pigeonhole.transactions",
        Counter,
        false,
        "{transaction}",
        [attr::OUTCOME],
        "Transactions committed (outcome committed) and refused with Conflict (outcome conflict).",
        |s, out| {
            out.push((Value::U(s.h.transactions), kv(attr::OUTCOME, "committed")));
            out.push((
                Value::U(s.h.transaction_conflicts),
                kv(attr::OUTCOME, "conflict"),
            ));
        }
    ),
    spec!(
        "pigeonhole.transaction.retries",
        Counter,
        false,
        "{retry}",
        [],
        "Attempts Pigeonhole::transact retried.",
        |s, out| one(out, s.h.transaction_retries)
    ),
    spec!(
        "pigeonhole.cross_shard.rounds",
        Counter,
        false,
        "{round}",
        [],
        "Two-phase rounds of cross-shard commits decided.",
        |s, out| one(out, s.e.cross_rounds)
    ),
    spec!(
        "pigeonhole.cross_shard.failed_fast",
        Counter,
        false,
        "{commit}",
        [],
        "Cross-shard commits that failed with Conflict without another round.",
        |s, out| one(out, s.e.cross_failed_fast)
    ),
    // Write stalls and background work.
    spec!(
        "pigeonhole.write_stalls",
        Counter,
        false,
        "{stall}",
        [attr::STALL_REASON],
        "Write stalls: commits held for memtable arena room (reason room) or paced because L0 was too deep (reason pacing).",
        |s, out| {
            out.push((Value::U(s.e.room_stalls), kv(attr::STALL_REASON, "room")));
            out.push((
                Value::U(s.e.pacing_stalls),
                kv(attr::STALL_REASON, "pacing"),
            ));
        }
    ),
    spec!(
        "pigeonhole.write_stall.time",
        Counter,
        true,
        "s",
        [attr::STALL_REASON],
        "Time the shards spent in write stalls, summed over shards; its rate is the fraction of time stalled.",
        |s, out| {
            out.push((
                seconds(s.e.room_stall_nanos),
                kv(attr::STALL_REASON, "room"),
            ));
            out.push((
                seconds(s.e.pacing_stall_nanos),
                kv(attr::STALL_REASON, "pacing"),
            ));
        }
    ),
    spec!(
        "pigeonhole.flushes",
        Counter,
        false,
        "{flush}",
        [attr::OUTCOME],
        "Memtable flushes completed (outcome success) and failed (outcome failure; retried, nothing lost).",
        |s, out| {
            out.push((Value::U(s.e.flushes), kv(attr::OUTCOME, "success")));
            out.push((Value::U(s.e.flush_failures), kv(attr::OUTCOME, "failure")));
        }
    ),
    spec!(
        "pigeonhole.compactions",
        Counter,
        false,
        "{compaction}",
        [attr::OUTCOME],
        "Compactions completed (success), failed or unable to start (failure), and taken back from an application's worker (reclaimed).",
        |s, out| {
            out.push((Value::U(s.e.compactions), kv(attr::OUTCOME, "success")));
            out.push((
                Value::U(s.e.compaction_failures),
                kv(attr::OUTCOME, "failure"),
            ));
            out.push((
                Value::U(s.e.compactions_reclaimed),
                kv(attr::OUTCOME, "reclaimed"),
            ));
        }
    ),
    spec!(
        "pigeonhole.background.take_backs",
        Counter,
        false,
        "{task}",
        [attr::TAKE_BACK_REASON],
        "Background tasks the shards took back from application-owned workers: not driven for 10 ms (reason undriven), or held while a commit waited for room (reason room). Nonzero means the workers aren't driven.",
        |s, out| {
            out.push((
                Value::U(s.e.take_back_undriven),
                kv(attr::TAKE_BACK_REASON, "undriven"),
            ));
            out.push((
                Value::U(s.e.take_back_room),
                kv(attr::TAKE_BACK_REASON, "room"),
            ));
        }
    ),
    spec!(
        "pigeonhole.shard.busy.time",
        Counter,
        true,
        "s",
        [],
        "Time the engine-owned shard threads spent working, summed over shards; its rate is how many shards' worth of work the database did.",
        |s, out| out.push((seconds(s.e.shard_busy_nanos), Vec::new()))
    ),
    spec!(
        "pigeonhole.memtable.size",
        Gauge,
        false,
        "By",
        [],
        "Memtable arena bytes in use, summed over shards: live memtables in whole chunks, and flushed ones a snapshot or read still holds. Near the limit, commits wait for room.",
        |s, out| one(out, s.m.memtable_bytes)
    ),
    spec!(
        "pigeonhole.memtable.limit",
        Gauge,
        false,
        "By",
        [],
        "The memtable arenas' size, summed over shards: Options::memtable_budget (per shard, rounded up to 2 MiB) times the shard count.",
        |s, out| one(out, s.m.memtable_limit_bytes)
    ),
    // Caches.
    spec!(
        "pigeonhole.block_cache.requests",
        Counter,
        false,
        "{request}",
        [attr::CACHE_RESULT],
        "Block cache lookups that found the block (hit) or read it from the file (miss).",
        |s, out| {
            out.push((Value::U(s.e.block_hits), kv(attr::CACHE_RESULT, "hit")));
            out.push((Value::U(s.e.block_misses), kv(attr::CACHE_RESULT, "miss")));
        }
    ),
    spec!(
        "pigeonhole.block_cache.capacity",
        Gauge,
        false,
        "By",
        [],
        "The block cache's capacity.",
        |s, out| one(out, s.m.block_cache_capacity_bytes)
    ),
    spec!(
        "pigeonhole.block_cache.pinned",
        Gauge,
        false,
        "By",
        [],
        "SST indexes and filters pinned in the block cache; past half the capacity, reads start costing two reads.",
        |s, out| one(out, s.m.block_cache_pinned_bytes)
    ),
    spec!(
        "pigeonhole.block_cache.pinned_warnings",
        Counter,
        false,
        "{warning}",
        [],
        "Times the pinned bytes rose past half the block cache's capacity.",
        |s, out| one(out, s.e.pinned_warnings)
    ),
    spec!(
        "pigeonhole.row_cache.requests",
        Counter,
        false,
        "{request}",
        [attr::CACHE_RESULT],
        "Row cache lookups that found the row (hit) or not (miss).",
        |s, out| {
            out.push((Value::U(s.e.row_hits), kv(attr::CACHE_RESULT, "hit")));
            out.push((Value::U(s.e.row_misses), kv(attr::CACHE_RESULT, "miss")));
        }
    ),
    spec!(
        "pigeonhole.row_cache.fills",
        Counter,
        false,
        "{fill}",
        [attr::FILL_SOURCE],
        "Rows cached after a read (source read) or written through by a commit (source write).",
        |s, out| {
            out.push((Value::U(s.e.row_fills), kv(attr::FILL_SOURCE, "read")));
            out.push((
                Value::U(s.e.row_write_throughs),
                kv(attr::FILL_SOURCE, "write"),
            ));
        }
    ),
    // File and storage.
    spec!(
        "pigeonhole.file.size",
        Gauge,
        false,
        "By",
        [],
        "The database file's length.",
        |s, out| if let Some(i) = &s.info {
            one(out, i.file_bytes)
        }
    ),
    spec!(
        "pigeonhole.file.free",
        Gauge,
        false,
        "By",
        [],
        "Bytes of the file free for reuse.",
        |s, out| if let Some(f) = s.info.as_ref().and_then(|i| i.free_bytes) {
            one(out, f)
        }
    ),
    spec!(
        "pigeonhole.file.growths",
        Counter,
        false,
        "{growth}",
        [],
        "Times the file grew because the page allocator ran out of free space.",
        |s, out| one(out, s.e.file_growths)
    ),
    spec!(
        "pigeonhole.file.growth.time",
        Counter,
        true,
        "s",
        [],
        "Time those growths held the page allocator.",
        |s, out| out.push((seconds(s.e.file_growth_nanos), Vec::new()))
    ),
    spec!(
        "pigeonhole.sst.count",
        Gauge,
        false,
        "{sst}",
        [attr::LEVEL, attr::TABLE, attr::FAMILY],
        "SSTs per LSM level; level 0's count is the compaction-debt signal. Table and family only with Config::storage_by_table.",
        |s, out| storage(s, out, |l| l.ssts)
    ),
    spec!(
        "pigeonhole.sst.size",
        Gauge,
        false,
        "By",
        [attr::LEVEL, attr::TABLE, attr::FAMILY],
        "SST bytes per LSM level. Table and family only with Config::storage_by_table.",
        |s, out| storage(s, out, |l| l.bytes)
    ),
    spec!(
        "pigeonhole.blob.size",
        Gauge,
        false,
        "By",
        [attr::BLOB_STATE, attr::TABLE, attr::FAMILY],
        "Blob file bytes (state total) and the live bytes in them (state live); the gap is what blob GC can reclaim.",
        blobs
    ),
    // Readers, threads and I/O.
    spec!(
        "pigeonhole.reader_slots.used",
        Gauge,
        false,
        "{slot}",
        [],
        "Reader processes attached.",
        |s, out| one(out, s.m.reader_slots_active)
    ),
    spec!(
        "pigeonhole.reader_slots.limit",
        Gauge,
        false,
        "{slot}",
        [],
        "The reader slot count.",
        |s, out| one(out, s.m.reader_slots)
    ),
    spec!(
        "pigeonhole.reader_slots.reclaimed",
        Counter,
        false,
        "{slot}",
        [],
        "Reader slots freed because their process died without closing.",
        |s, out| one(out, s.e.reader_slots_reclaimed)
    ),
    spec!(
        "pigeonhole.reader.seqno_lag",
        Gauge,
        false,
        "{commit}",
        [],
        "How many commits behind the newest the oldest reader snapshot is; a growing value holds back reclamation.",
        |s, out| one(out, s.m.reader_pin_seqno_lag)
    ),
    spec!(
        "pigeonhole.threads",
        Gauge,
        false,
        "{thread}",
        [attr::THREAD_PINNED],
        "Threads the engine started, pinned to a CPU of their own or not.",
        |s, out| {
            out.push((
                Value::U(s.m.threads_pinned),
                vec![KeyValue::new(attr::THREAD_PINNED, true)],
            ));
            out.push((
                Value::U(s.m.threads_started.saturating_sub(s.m.threads_pinned)),
                vec![KeyValue::new(attr::THREAD_PINNED, false)],
            ));
        }
    ),
    spec!(
        "pigeonhole.async.sync_reads",
        Counter,
        false,
        "{read}",
        [],
        "File reads made synchronously inside async reads, blocking their executor thread.",
        |s, out| one(out, s.e.async_sync_reads)
    ),
    spec!(
        "pigeonhole.io.rings",
        Gauge,
        false,
        "{ring}",
        [],
        "io_uring rings the database has (io_uring only).",
        |s, out| if let Some(r) = &s.io_rings {
            one(out, r.rings as u64)
        }
    ),
    spec!(
        "pigeonhole.io.ring_attach_failures",
        Counter,
        false,
        "{failure}",
        [],
        "Times a thread could not get an io_uring ring (io_uring only).",
        |s, out| if s.io_rings.is_some() {
            one(out, s.h.io_attach_failures)
        }
    ),
    spec!(
        "pigeonhole.io.inline",
        Counter,
        false,
        "{op}",
        [],
        "I/O run inline on the calling thread instead of through io_uring (io_uring only).",
        |s, out| if s.io_rings.is_some() {
            one(out, s.h.io_inline)
        }
    ),
    // The change feed.
    spec!(
        "pigeonhole.feed.syncs",
        Counter,
        false,
        "{sync}",
        [attr::FEED_SYNC_KIND],
        "Syncs the change feed made on its own (kind background) and of its sidecar file (kind sidecar); only while the feed is on.",
        |s, out| if s.feed_on() {
            out.push((
                Value::U(s.e.feed_background_syncs),
                kv(attr::FEED_SYNC_KIND, "background"),
            ));
            out.push((
                Value::U(s.e.feed_sidecar_syncs),
                kv(attr::FEED_SYNC_KIND, "sidecar"),
            ));
        }
    ),
    spec!(
        "pigeonhole.feed.retained.size",
        Gauge,
        false,
        "By",
        [],
        "WAL bytes the change feed retains.",
        |s, out| if let Some(f) = s.info.as_ref().and_then(|i| i.feed) {
            one(out, f.retained_wal_bytes)
        }
    ),
    spec!(
        "pigeonhole.feed.retained.limit",
        Gauge,
        false,
        "By",
        [],
        "The change feed's retention limit; near it, slow subscribers start expiring.",
        |s, out| if let Some(f) = s.info.as_ref().and_then(|i| i.feed) {
            one(out, f.retention_bytes)
        }
    ),
    spec!(
        "pigeonhole.feed.held_blobs.size",
        Gauge,
        false,
        "By",
        [],
        "Blob bytes the change feed's retained history holds.",
        |s, out| if let Some(f) = s.info.as_ref().and_then(|i| i.feed) {
            one(out, f.held_blob_bytes)
        }
    ),
    spec!(
        "pigeonhole.feed.lag",
        Gauge,
        false,
        "{commit}",
        [attr::FEED_NAME],
        "Seqnos between the database's visible seqno and a watched feed's covered position (Registration::watch_feed): about one a commit, so near 0 while the feed keeps up and growing while its subscriber falls behind or after it failed.",
        |s, out| for (name, lag) in &s.feed_lags {
            out.push((
                Value::U(*lag),
                vec![KeyValue::new(attr::FEED_NAME, name.clone())],
            ));
        }
    ),
    // Health.
    spec!(
        "pigeonhole.failed",
        Gauge,
        false,
        "1",
        [],
        "0 or 1: 1 once the instance has failed (Pigeonhole::is_failed). The first alert to set.",
        |s, out| one(out, s.failed as u64)
    ),
    spec!(
        "pigeonhole.recoveries",
        Counter,
        false,
        "{recovery}",
        [],
        "Automatic recoveries completed.",
        |s, out| one(out, s.h.recoveries)
    ),
    spec!(
        "pigeonhole.recovery.gave_up",
        Gauge,
        false,
        "1",
        [],
        "0 or 1: 1 once automatic recovery gave up.",
        |s, out| one(out, s.gave_up as u64)
    ),
];

/// The SST gauges: per level, or per table, family and level.
fn storage(s: &Snapshot, out: &mut Points, f: fn(&pigeonhole::LevelInfo) -> u64) {
    let Some(info) = &s.info else { return };
    let mut by_level: std::collections::BTreeMap<u8, u64> = Default::default();
    for t in &info.tables {
        for fam in &t.storage {
            for l in &fam.levels {
                if s.storage_by_table {
                    out.push((
                        Value::U(f(l)),
                        vec![
                            KeyValue::new(attr::LEVEL, i64::from(l.level)),
                            KeyValue::new(attr::TABLE, t.name.clone()),
                            KeyValue::new(attr::FAMILY, fam.family.clone()),
                        ],
                    ));
                } else {
                    *by_level.entry(l.level).or_default() += f(l);
                }
            }
        }
    }
    for (level, v) in by_level {
        out.push((
            Value::U(v),
            vec![KeyValue::new(attr::LEVEL, i64::from(level))],
        ));
    }
}

/// The blob gauge: in total, or per table and family.
fn blobs(s: &Snapshot, out: &mut Points) {
    let Some(info) = &s.info else { return };
    let (mut total, mut live) = (0, 0);
    for t in &info.tables {
        for fam in &t.storage {
            if s.storage_by_table {
                for (state, v) in [("total", fam.blob_bytes), ("live", fam.blob_live_bytes)] {
                    out.push((
                        Value::U(v),
                        vec![
                            KeyValue::new(attr::BLOB_STATE, state),
                            KeyValue::new(attr::TABLE, t.name.clone()),
                            KeyValue::new(attr::FAMILY, fam.family.clone()),
                        ],
                    ));
                }
            } else {
                total += fam.blob_bytes;
                live += fam.blob_live_bytes;
            }
        }
    }
    if !s.storage_by_table {
        out.push((Value::U(total), kv(attr::BLOB_STATE, "total")));
        out.push((Value::U(live), kv(attr::BLOB_STATE, "live")));
    }
}

/// What one reading of the database gives the counters, before carrying.
struct Raw {
    m: EngineMetrics,
    row: pigeonhole::RowCacheStats,
    recoveries: u64,
    io: Option<IoRings>,
}

/// Counters, each named once: the struct, its reading from [`Raw`], and its sum.
macro_rules! counters {
    ($name:ident { $($field:ident: $read:expr,)* }) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        struct $name {
            $($field: u64,)*
        }

        impl $name {
            fn read(raw: &Raw) -> Self {
                Self { $($field: ($read)(raw),)* }
            }

            fn plus(&self, other: &Self) -> Self {
                Self { $($field: self.$field + other.$field,)* }
            }
        }
    };
}

/// The engine's counters: they restart from 0 when an automatic recovery starts a new engine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EngineCounters {
    commits: [u64; 4],
    rest: EngineRest,
}

impl EngineCounters {
    fn read(raw: &Raw) -> Self {
        Self {
            commits: DURABILITIES.map(|(d, _)| raw.m.commits(d).count),
            rest: EngineRest::read(raw),
        }
    }

    fn plus(&self, other: &Self) -> Self {
        Self {
            commits: std::array::from_fn(|i| self.commits[i] + other.commits[i]),
            rest: self.rest.plus(&other.rest),
        }
    }
}

impl std::ops::Deref for EngineCounters {
    type Target = EngineRest;
    fn deref(&self) -> &EngineRest {
        &self.rest
    }
}

counters!(EngineRest {
    flushes: |r: &Raw| r.m.flushes,
    flush_failures: |r: &Raw| r.m.flush_failures,
    compactions: |r: &Raw| r.m.compactions,
    compaction_failures: |r: &Raw| r.m.compaction_failures,
    compactions_reclaimed: |r: &Raw| r.m.compactions_reclaimed,
    take_back_undriven: |r: &Raw| r.m.background_fallback_take_backs,
    take_back_room: |r: &Raw| r.m.background_room_take_backs,
    room_stalls: |r: &Raw| r.m.room_stalls,
    room_stall_nanos: |r: &Raw| r.m.room_stall_nanos,
    pacing_stalls: |r: &Raw| r.m.pacing_stalls,
    pacing_stall_nanos: |r: &Raw| r.m.pacing_stall_nanos,
    block_hits: |r: &Raw| r.m.block_cache_hits,
    block_misses: |r: &Raw| r.m.block_cache_misses,
    pinned_warnings: |r: &Raw| r.m.block_cache_pinned_warnings,
    file_growths: |r: &Raw| r.m.file_growths,
    file_growth_nanos: |r: &Raw| r.m.file_growth_nanos,
    async_sync_reads: |r: &Raw| r.m.async_sync_reads,
    shard_busy_nanos: |r: &Raw| r.m.shard_busy_nanos,
    reader_slots_reclaimed: |r: &Raw| r.m.reader_slots_reclaimed,
    cross_rounds: |r: &Raw| r.m.cross_shard.rounds,
    cross_failed_fast: |r: &Raw| r.m.cross_shard.failed_fast,
    group_syncs: |r: &Raw| r.m.group_syncs,
    group_sync_nanos: |r: &Raw| r.m.group_sync_nanos,
    feed_background_syncs: |r: &Raw| r.m.feed_background_syncs,
    feed_sidecar_syncs: |r: &Raw| r.m.feed_sidecar_syncs,
    row_hits: |r: &Raw| r.row.hits,
    row_misses: |r: &Raw| r.row.misses,
    row_fills: |r: &Raw| r.row.fills,
    row_write_throughs: |r: &Raw| r.row.write_throughs,
});

// The handle's counters: they span automatic recoveries, and restart only with a reopen.
counters!(HandleCounters {
    transactions: |r: &Raw| r.m.transactions,
    transaction_conflicts: |r: &Raw| r.m.transaction_conflicts,
    transaction_retries: |r: &Raw| r.m.transaction_retries,
    recoveries: |r: &Raw| r.recoveries,
    io_attach_failures: |r: &Raw| r.io.map_or(0, |i| i.attach_failures),
    io_inline: |r: &Raw| r.io.map_or(0, |i| i.inline),
});

/// One reading, shared by every instrument's callback in a collection.
struct Snapshot {
    at: Instant,
    /// The counters, carried so that each only grows within the registration.
    e: EngineCounters,
    h: HandleCounters,
    /// The reading itself, for the gauges.
    m: EngineMetrics,
    info: Option<DatabaseInfo>,
    io_rings: Option<IoRings>,
    failed: bool,
    gave_up: bool,
    /// Seconds, per durability and quantile, over the latency window.
    latency: [[Option<f64>; 3]; 4],
    storage_by_table: bool,
    /// Each watched feed's lag in commits, by name.
    feed_lags: Vec<(String, u64)>,
}

impl Snapshot {
    fn feed_on(&self) -> bool {
        self.info.as_ref().is_some_and(|i| i.feed.is_some())
    }
}

/// How long one reading serves: every instrument's callback in one collection reuses it.
const REUSE: Duration = Duration::from_millis(100);

struct Shared {
    name: KeyValue,
    storage_by_table: bool,
    window: Duration,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic in a callback can't leave the state half-written in a way that matters:
        // the worst is one reading's counters, which the next reading replaces.
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The current reading, or `None` once the registration is dropped or the database
    /// closed.
    fn snapshot(&self) -> Option<Arc<Snapshot>> {
        let mut state = self.lock();
        let now = Instant::now();
        if let Some(c) = &state.cached
            && now.duration_since(c.at) < REUSE
        {
            return Some(Arc::clone(c));
        }
        let snap = Arc::new(state.read(now, self.window, self.storage_by_table)?);
        state.cached = Some(Arc::clone(&snap));
        Some(snap)
    }
}

struct State {
    db: Option<Pigeonhole>,
    cached: Option<Arc<Snapshot>>,
    /// The engine's generation at the last reading: a change is an automatic recovery.
    generation: u64,
    /// Added to the engine's and the handle's counters, from the engines and handles before.
    engine_base: EngineCounters,
    handle_base: HandleCounters,
    /// The last reading's counters, before carrying.
    last_engine: EngineCounters,
    last_handle: HandleCounters,
    /// Commit latency readings, oldest first, back to the window's start.
    latency: VecDeque<(Instant, [LatencyHistogram; 4])>,
    /// The feeds `Registration::watch_feed` watches, by name.
    feeds: Vec<(String, pigeonhole::FeedProgress)>,
}

impl State {
    fn new(db: &Pigeonhole) -> Self {
        let mut state = Self {
            db: Some(db.clone()),
            cached: None,
            generation: db.recovery_stats().generation,
            engine_base: Default::default(),
            handle_base: Default::default(),
            last_engine: Default::default(),
            last_handle: Default::default(),
            latency: VecDeque::new(),
            feeds: Vec::new(),
        };
        // The window's first baseline, so the first collection already has an interval.
        state
            .latency
            .push_back((Instant::now(), latency(&db.engine_metrics())));
        state
    }

    fn rebind(&mut self, db: &Pigeonhole) {
        self.engine_base = self.engine_base.plus(&self.last_engine);
        self.handle_base = self.handle_base.plus(&self.last_handle);
        self.last_engine = Default::default();
        self.last_handle = Default::default();
        self.generation = db.recovery_stats().generation;
        self.latency.clear();
        self.latency
            .push_back((Instant::now(), latency(&db.engine_metrics())));
        self.cached = None;
        self.db = Some(db.clone());
    }

    fn read(&mut self, now: Instant, window: Duration, storage_by_table: bool) -> Option<Snapshot> {
        // A clone (two `Arc`s), so the reading can update the state while it holds the handle.
        let db = self.db.clone()?;
        // A closed database reports nothing.
        if db.is_closed() {
            return None;
        }
        let info = db.info().ok();
        let recovery = db.recovery_stats();
        let raw = Raw {
            m: db.engine_metrics(),
            row: db.row_cache_stats(),
            recoveries: recovery.recoveries,
            io: db.io_rings(),
        };
        let (e, h) = self.absorb(
            recovery.generation,
            EngineCounters::read(&raw),
            HandleCounters::read(&raw),
        );
        let hist = latency(&raw.m);
        let latency = self.quantiles(now, window, &hist);
        // The visible seqno after each feed's position: a feed read meanwhile can only be
        // further on, so the lag never reads below zero for long, and is clamped at it.
        self.feeds.retain(|(_, p)| p.is_live());
        let positions: Vec<_> = self
            .feeds
            .iter()
            .map(|(n, p)| (n.clone(), p.covered_seqno()))
            .collect();
        let visible = db.visible_seqno();
        let feed_lags = positions
            .into_iter()
            .map(|(n, d)| (n, visible.saturating_sub(d)))
            .collect();
        Some(Snapshot {
            at: now,
            e,
            h,
            io_rings: raw.io,
            m: raw.m,
            info,
            failed: db.is_failed(),
            gave_up: recovery.gave_up,
            latency,
            storage_by_table,
            feed_lags,
        })
    }

    /// Takes one reading's counters, of the engine of `generation`, and returns them carried:
    /// each counter is its value plus what the engines and handles before this one reached.
    fn absorb(
        &mut self,
        generation: u64,
        engine: EngineCounters,
        handle: HandleCounters,
    ) -> (EngineCounters, HandleCounters) {
        if generation != self.generation {
            // A new engine (an automatic recovery): its counters start again from 0, so the
            // old engine's last reading becomes part of the base, and the latency window,
            // whose histograms restarted too, starts over.
            self.engine_base = self.engine_base.plus(&self.last_engine);
            self.generation = generation;
            self.latency.clear();
        }
        self.last_engine = engine;
        self.last_handle = handle;
        (
            self.engine_base.plus(&self.last_engine),
            self.handle_base.plus(&self.last_handle),
        )
    }

    /// Each level's quantiles over the commits since the window's start, recording `now`'s
    /// reading for later windows.
    fn quantiles(
        &mut self,
        now: Instant,
        window: Duration,
        hist: &[LatencyHistogram; 4],
    ) -> [[Option<f64>; 3]; 4] {
        // The baseline is the newest reading at least `window` old (or the oldest there is,
        // while the registration is younger than the window); older ones are no longer needed.
        while self.latency.len() > 1 && now.duration_since(self.latency[1].0) >= window {
            self.latency.pop_front();
        }
        let base = self.latency.front().map(|(_, h)| *h);
        self.latency.push_back((now, *hist));
        let Some(base) = base else {
            return [[None; 3]; 4];
        };
        std::array::from_fn(|d| {
            let interval = hist[d].since(&base[d]);
            QUANTILES.map(|(q, _)| interval.quantile_upper_nanos(q).map(|n| n as f64 / 1e9))
        })
    }
}

fn latency(m: &EngineMetrics) -> [LatencyHistogram; 4] {
    DURABILITIES.map(|(d, _)| m.commit_latency(d))
}

/// Builds `spec`'s instrument with its callback.
fn build(
    spec: &'static Spec,
    meter: &Meter,
    shared: &Arc<Shared>,
) -> Box<dyn std::any::Any + Send + Sync> {
    let i = &spec.info;
    let observe = {
        let shared = Arc::clone(shared);
        move |emit: &mut dyn FnMut(Value, &[KeyValue])| {
            let Some(snap) = shared.snapshot() else {
                return;
            };
            let mut points = Vec::new();
            (spec.read)(&snap, &mut points);
            for (v, mut attrs) in points {
                attrs.insert(0, shared.name.clone());
                emit(v, &attrs);
            }
        }
    };
    let f64_of = |v: Value| match v {
        Value::F(f) => f,
        Value::U(u) => u as f64,
    };
    let u64_of = |v: Value| match v {
        Value::U(u) => u,
        Value::F(f) => f as u64,
    };
    match (i.kind, spec.float) {
        (InstrumentKind::Counter, false) => Box::new(
            meter
                .u64_observable_counter(i.name)
                .with_unit(i.unit)
                .with_description(i.description)
                .with_callback(move |o: &dyn AsyncInstrument<u64>| {
                    observe(&mut |v, a| o.observe(u64_of(v), a))
                })
                .build(),
        ),
        (InstrumentKind::Counter, true) => Box::new(
            meter
                .f64_observable_counter(i.name)
                .with_unit(i.unit)
                .with_description(i.description)
                .with_callback(move |o: &dyn AsyncInstrument<f64>| {
                    observe(&mut |v, a| o.observe(f64_of(v), a))
                })
                .build(),
        ),
        (InstrumentKind::Gauge, false) => Box::new(
            meter
                .u64_observable_gauge(i.name)
                .with_unit(i.unit)
                .with_description(i.description)
                .with_callback(move |o: &dyn AsyncInstrument<u64>| {
                    observe(&mut |v, a| o.observe(u64_of(v), a))
                })
                .build(),
        ),
        (InstrumentKind::Gauge, true) => Box::new(
            meter
                .f64_observable_gauge(i.name)
                .with_unit(i.unit)
                .with_description(i.description)
                .with_callback(move |o: &dyn AsyncInstrument<f64>| {
                    observe(&mut |v, a| o.observe(f64_of(v), a))
                })
                .build(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_metric_is_named_once_and_follows_the_naming_rules() {
        let mut seen = std::collections::HashSet::new();
        for m in metrics() {
            assert!(seen.insert(m.name), "{} twice", m.name);
            assert!(m.name.starts_with("pigeonhole."), "{}", m.name);
            assert!(
                m.name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '.' || c == '_'),
                "{}",
                m.name
            );
            for a in m.attributes {
                assert!(a.starts_with("pigeonhole."), "{}: attribute {a}", m.name);
            }
            // D316: counters of seconds end in `.time`; seconds are `s`, bytes `By`.
            if m.kind == InstrumentKind::Counter && m.unit == "s" {
                assert!(m.name.ends_with(".time"), "{}", m.name);
            }
            assert!(
                ["s", "By", "1"].contains(&m.unit)
                    || (m.unit.starts_with('{') && m.unit.ends_with('}')),
                "{}: unit {}",
                m.name,
                m.unit
            );
        }
    }

    /// A state with no database, for the carry arithmetic alone.
    fn bare_state(generation: u64) -> State {
        State {
            db: None,
            cached: None,
            generation,
            engine_base: Default::default(),
            handle_base: Default::default(),
            last_engine: Default::default(),
            last_handle: Default::default(),
            latency: VecDeque::new(),
            feeds: Vec::new(),
        }
    }

    fn engine(commits: u64, flushes: u64) -> EngineCounters {
        EngineCounters {
            commits: [0, 0, commits, 0],
            rest: EngineRest {
                flushes,
                ..Default::default()
            },
        }
    }

    fn handle(transactions: u64) -> HandleCounters {
        HandleCounters {
            transactions,
            ..Default::default()
        }
    }

    #[test]
    fn counters_stay_monotonic_across_a_recovery() {
        let mut s = bare_state(1);
        let (e, h) = s.absorb(1, engine(10, 2), handle(5));
        assert_eq!((e.commits[2], e.flushes, h.transactions), (10, 2, 5));
        let (e, _) = s.absorb(1, engine(15, 3), handle(6));
        assert_eq!((e.commits[2], e.flushes), (15, 3));
        // Generation 2: the new engine's counters restart from 0; the handle's span it.
        let (e, h) = s.absorb(2, engine(1, 0), handle(7));
        assert_eq!((e.commits[2], e.flushes, h.transactions), (16, 3, 7));
        let (e, h) = s.absorb(2, engine(4, 1), handle(9));
        assert_eq!((e.commits[2], e.flushes, h.transactions), (19, 4, 9));
        // Another recovery adds the second engine's last reading too.
        let (e, _) = s.absorb(3, engine(0, 0), handle(9));
        assert_eq!((e.commits[2], e.flushes), (19, 4));
    }

    #[test]
    fn prometheus_names_follow_the_exporter() {
        let by_name = |n: &str| metrics().find(|m| m.name == n).unwrap().prometheus_name();
        assert_eq!(
            by_name("pigeonhole.write_stall.time"),
            "pigeonhole_write_stall_time_seconds_total"
        );
        assert_eq!(
            by_name("pigeonhole.file.size"),
            "pigeonhole_file_size_bytes"
        );
        assert_eq!(by_name("pigeonhole.commits"), "pigeonhole_commits_total");
        assert_eq!(by_name("pigeonhole.failed"), "pigeonhole_failed_ratio");
    }
}
