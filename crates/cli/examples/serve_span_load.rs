//! A cluster load with transactions across partitions (D329) and change-feed subscribers
//! (#2221), against a running `phdb serve` cluster.
//!
//! The cluster is the caller's: `--server URL` once per node, and the load spreads its callers
//! over them. The table `--table` (family `f`) must already be split across the partitions,
//! with one row prefix per partition (`--parts`, by default `a,b,c`, for split keys `b` and
//! `c`). Rows are `<prefix><10 digits>`, below `--rows` per partition.
//!
//! Each caller runs one operation at a time:
//! - **single:** a write to one row of one partition. A `Mutate` (`--single mutate`), or a
//!   transaction (`--single txn`): `Begin` routed to the row, then `Commit` with the row.
//! - **spanning** (`--span-share` percent of operations): a transaction begun with
//!   `spans_partitions`, homed on a random partition, committing one row in each of
//!   `--span-parts` partitions (the home first), so every commit prepares external shares
//!   on its branches.
//! - **conflict** (`--conflict` percent of transactions): the transaction first reads one of
//!   `--hot-keys` rows of its home partition (`Get`), and writes that row, so concurrent ones
//!   fail OCC with `Conflict`.
//!
//! Latency is the whole operation, from its send (closed loop) or its due time (with
//! `--rate`, open loop) to the reply. Outcomes: committed, `Conflict`, `OutcomeUnknown` (then
//! `Outcome` is asked, and its state counted), other errors by code, and spanning parts the
//! reply marks not yet applied.
//!
//! `--subscribers N` follows the table's change feed with N cluster subscriptions, spread
//! over the nodes, each over every partition: N readers per partition. One decodes its events
//! for the feed lag (send to arrival; each value's first 8 bytes are its send time); the
//! others count frames.
//!
//! `--stall-pid PID --stall-at S --stall-for S` sends SIGSTOP to a node at S seconds into the
//! measured phase and SIGCONT after the second S, so shares prepared just before stay in
//! doubt, the feed holds them, and the resolver runs once the node is back (#2221's H shape;
//! reported, not judged).
//!
//! Before and after the measured phase it reads each node's `/metrics` through
//! `Admin.Metrics`: the external-share counters (ICR 0148) as deltas, and, every second, the
//! in-doubt gauges, keeping their peak. A run with spanning transactions whose prepares stay
//! at 0 prints `FUNCTIONAL FAILED`.
//!
//! The last line is `RESULT key=value ...`, for scripts.
//!
//! ```text
//! cargo build --release -p pigeonhole-cli --features serve --bin phdb --example serve_span_load
//! target/release/examples/serve_span_load --server http://127.0.0.1:7471 \
//!     --server http://127.0.0.1:7472 --server http://127.0.0.1:7473 --shape m
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
mod proto {
    include!("../src/serve/proto/pigeonhole.v1.rs");
}

use proto::data_client::DataClient;
use proto::feed_client::FeedClient;
use proto::transactions_client::TransactionsClient;

#[derive(Parser, Debug, Clone)]
struct Args {
    /// A node of the cluster, as `http://HOST:PORT`; repeat for each node.
    #[arg(long = "server", required = true)]
    servers: Vec<String>,
    /// The nodes' process ids, for their CPU per operation (Linux, same host); repeatable.
    #[arg(long = "server-pid")]
    server_pids: Vec<u32>,
    /// A preset of the settings below (#2221's shapes); the explicit flags override it.
    #[arg(long, value_enum)]
    shape: Option<Shape>,
    /// The table, split across the partitions, with family `f`.
    #[arg(long, default_value = "span")]
    table: String,
    /// Each partition's row prefix, in the split keys' order.
    #[arg(long, value_delimiter = ',', default_value = "a,b,c")]
    parts: Vec<String>,
    /// Rows per partition the load writes to.
    #[arg(long, default_value_t = 1_000_000)]
    rows: u64,
    /// Concurrent callers.
    #[arg(long)]
    writers: Option<usize>,
    /// Operations per second across the callers, open loop; without it each caller sends its
    /// next operation as soon as the last answers.
    #[arg(long)]
    rate: Option<f64>,
    /// Percent of operations that are spanning transactions.
    #[arg(long)]
    span_share: Option<f64>,
    /// Partitions a spanning transaction writes, its home included (2 or more).
    #[arg(long)]
    span_parts: Option<usize>,
    /// Percent of transactions that read and write a hot row.
    #[arg(long)]
    conflict: Option<f64>,
    /// Hot rows per partition, for `--conflict`.
    #[arg(long, default_value_t = 100)]
    hot_keys: u64,
    /// How a single-partition write goes.
    #[arg(long, value_enum)]
    single: Option<Single>,
    /// Change-feed subscriptions (each follows every partition).
    #[arg(long)]
    subscribers: Option<usize>,
    /// Each write's durability (spanning commits are `GroupSync` at least).
    #[arg(long, value_enum, default_value_t = WriteDurability::GroupSync)]
    durability: WriteDurability,
    /// Bytes per value (at least 8: the send time).
    #[arg(long, default_value_t = 100)]
    value_bytes: usize,
    /// Measured seconds.
    #[arg(long, default_value_t = 90)]
    seconds: u64,
    /// Seconds of the same load first, discarded.
    #[arg(long, default_value_t = 15)]
    warmup_secs: u64,
    /// gRPC connections per node, shared by the callers.
    #[arg(long, default_value_t = 4)]
    conns: usize,
    /// A node's process to stop (SIGSTOP) during the measured phase.
    #[arg(long, requires_all = ["stall_at", "stall_for"])]
    stall_pid: Option<u32>,
    /// Seconds into the measured phase to stop it.
    #[arg(long)]
    stall_at: Option<u64>,
    /// Seconds to keep it stopped.
    #[arg(long)]
    stall_for: Option<u64>,
    /// Run this process on these CPUs, e.g. `12-15,28-31` (Linux).
    #[arg(long, value_name = "LIST")]
    client_cpus: Option<String>,
    /// A label for the RESULT line.
    #[arg(long, default_value = "")]
    label: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Shape {
    /// 256 callers, closed loop, single-partition `Mutate`s: throughput (D335's base).
    T256,
    /// 64 callers, paced (`--rate` needed), single-partition `Mutate`s: latency.
    L,
    /// 64 callers, 10% spanning across 2 partitions, 1% conflicts, 16 subscribers.
    M,
    /// 64 callers, every operation spanning 3 partitions, 1% conflicts, 16 subscribers.
    S,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Single {
    Mutate,
    Txn,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum WriteDurability {
    Default,
    Buffered,
    GroupSync,
    Sync,
}

impl WriteDurability {
    fn proto(self) -> proto::Durability {
        match self {
            Self::Default => proto::Durability::Unspecified,
            Self::Buffered => proto::Durability::Buffered,
            Self::GroupSync => proto::Durability::GroupSync,
            Self::Sync => proto::Durability::Sync,
        }
    }
}

/// The settings a run uses: the shape's, with the explicit flags over them.
#[derive(Debug, Clone)]
struct Load {
    writers: usize,
    rate: Option<f64>,
    span_share: f64,
    span_parts: usize,
    conflict: f64,
    single: Single,
    subscribers: usize,
}

impl Load {
    fn of(args: &Args) -> Result<Self, String> {
        let (writers, span_share, span_parts, conflict, single, subscribers) = match args.shape {
            Some(Shape::T256) => (256, 0.0, 2, 0.0, Single::Mutate, 0),
            Some(Shape::L) => (64, 0.0, 2, 0.0, Single::Mutate, 0),
            Some(Shape::M) => (64, 10.0, 2, 1.0, Single::Txn, 16),
            Some(Shape::S) => (64, 100.0, 3, 1.0, Single::Txn, 16),
            None => (64, 0.0, 2, 0.0, Single::Mutate, 0),
        };
        let l = Self {
            writers: args.writers.unwrap_or(writers),
            rate: args.rate,
            span_share: args.span_share.unwrap_or(span_share),
            span_parts: args.span_parts.unwrap_or(span_parts),
            conflict: args.conflict.unwrap_or(conflict),
            single: args.single.unwrap_or(single),
            subscribers: args.subscribers.unwrap_or(subscribers),
        };
        if args.shape == Some(Shape::L) && l.rate.is_none() {
            return Err("--shape l needs --rate".into());
        }
        if l.span_share > 0.0 && (l.span_parts < 2 || l.span_parts > args.parts.len()) {
            return Err(format!(
                "--span-parts {} must be 2 to the {} partitions",
                l.span_parts,
                args.parts.len()
            ));
        }
        if l.writers == 0 {
            return Err("--writers must be at least 1".into());
        }
        Ok(l)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Some(list) = &args.client_cpus {
        pin_self(list)?;
    }
    let load = Load::of(&args)?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args, load))
}

#[cfg(target_os = "linux")]
fn pin_self(list: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = std::process::Command::new("taskset")
        .args(["-a", "-p", "-c", list, &std::process::id().to_string()])
        .stdout(std::process::Stdio::null())
        .status()?;
    if !status.success() {
        return Err(format!("taskset -c {list} failed").into());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_self(_list: &str) -> Result<(), Box<dyn std::error::Error>> {
    Err("--client-cpus is Linux-only".into())
}

/// A process's user plus system CPU seconds (Linux).
fn cpu_seconds(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks: f64 = f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?;
    Some(ticks / 100.0)
}

async fn connect(url: &str) -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match tonic::transport::Channel::from_shared(url.to_owned())?
            .connect()
            .await
        {
            Ok(c) => return Ok(c),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => return Err(format!("{url}: {e}").into()),
        }
    }
}

/// The stable code (ICR 0112) a gRPC status carries in its `ErrorDetail`, or its gRPC code.
fn api_code(status: &tonic::Status) -> String {
    use prost::Message;
    #[derive(Clone, PartialEq, Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<AnyMsg>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct AnyMsg {
        #[prost(string, tag = "1")]
        type_url: String,
        #[prost(bytes = "vec", tag = "2")]
        value: Vec<u8>,
    }
    RpcStatus::decode(status.details())
        .ok()
        .and_then(|s| {
            s.details
                .iter()
                .find(|d| d.type_url.ends_with("pigeonhole.v1.ErrorDetail"))
                .and_then(|d| proto::ErrorDetail::decode(d.value.as_slice()).ok())
        })
        .map_or_else(|| format!("{:?}", status.code()), |d| d.code)
}

/// When the run started: values carry their send time as nanoseconds since this, which the
/// lag subscriber, in the same process, compares with their arrival.
fn epoch() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

fn now_nanos() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

/// A small xorshift generator, one per caller.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    /// True `pct` percent of the time.
    fn chance(&mut self, pct: f64) -> bool {
        ((self.next() % 1_000_000) as f64) < pct * 10_000.0
    }
}

/// Latencies in microseconds, log-bucketed (about 5% wide) up to about 100 s.
#[derive(Clone)]
struct Hist(Vec<u64>);

impl Hist {
    const BUCKETS: usize = 400;

    fn new() -> Self {
        Self(vec![0; Self::BUCKETS])
    }

    fn record(&mut self, us: u64) {
        let b = ((((us + 1) as f64).ln() / 1.05f64.ln()) as usize).min(Self::BUCKETS - 1);
        self.0[b] += 1;
    }

    fn merge(&mut self, other: &Self) {
        for (a, b) in self.0.iter_mut().zip(&other.0) {
            *a += b;
        }
    }

    fn count(&self) -> u64 {
        self.0.iter().sum()
    }

    /// The `q` quantile's bucket, as its upper bound in microseconds (0 when empty).
    fn quantile(&self, q: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let want = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, n) in self.0.iter().enumerate() {
            seen += n;
            if seen >= want {
                return 1.05f64.powi(i as i32 + 1) as u64;
            }
        }
        0
    }
}

/// One kind of operation's tally.
#[derive(Clone)]
struct Kind {
    lat: Hist,
    committed: u64,
    conflict: u64,
    /// `OutcomeUnknown`, then `Outcome`'s state: committed, aborted, other.
    unknown: [u64; 3],
    /// Other errors, by code.
    errors: std::collections::BTreeMap<String, u64>,
    /// Spanning commits' parts the reply marked not yet applied.
    unapplied: u64,
}

impl Kind {
    fn new() -> Self {
        Self {
            lat: Hist::new(),
            committed: 0,
            conflict: 0,
            unknown: [0; 3],
            errors: Default::default(),
            unapplied: 0,
        }
    }

    fn merge(&mut self, o: &Self) {
        self.lat.merge(&o.lat);
        self.committed += o.committed;
        self.conflict += o.conflict;
        for (a, b) in self.unknown.iter_mut().zip(o.unknown) {
            *a += b;
        }
        for (k, v) in &o.errors {
            *self.errors.entry(k.clone()).or_default() += v;
        }
        self.unapplied += o.unapplied;
    }

    fn unknowns(&self) -> u64 {
        self.unknown.iter().sum()
    }

    fn other(&self) -> u64 {
        self.errors.values().sum()
    }
}

/// A caller's tally: single, spanning.
#[derive(Clone)]
struct Tally {
    single: Kind,
    spanning: Kind,
}

impl Tally {
    fn new() -> Self {
        Self {
            single: Kind::new(),
            spanning: Kind::new(),
        }
    }

    fn merge(&mut self, o: &Self) {
        self.single.merge(&o.single);
        self.spanning.merge(&o.spanning);
    }
}

/// What every caller shares.
struct Shared {
    args: Args,
    load: Load,
    /// Per node, its connections.
    conns: Vec<Vec<tonic::transport::Channel>>,
    /// Set while the phase measures; callers record only then.
    measuring: AtomicBool,
    stop: AtomicBool,
}

fn row_key(prefix: &str, n: u64) -> Vec<u8> {
    format!("{prefix}{n:010}").into_bytes()
}

fn value(bytes: usize) -> Vec<u8> {
    let mut v = vec![0x5a; bytes.max(8)];
    v[..8].copy_from_slice(&now_nanos().to_le_bytes());
    v
}

fn put_row(table: &str, row: Vec<u8>, value: Vec<u8>) -> proto::RowWrites {
    proto::RowWrites {
        row: Some(proto::RowKey {
            table: table.into(),
            row: row.into(),
        }),
        writes: vec![proto::Write {
            op: Some(proto::write::Op::Put(proto::Put {
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                timestamp: None,
                value: Some(proto::Value {
                    kind: Some(proto::value::Kind::Bytes(value.into())),
                }),
            })),
        }],
    }
}

/// How one operation ended.
enum Ended {
    Committed {
        unapplied: u64,
    },
    Conflict,
    /// `OutcomeUnknown`, then `Outcome`'s answer: 0 committed, 1 aborted, 2 other.
    Unknown(usize),
    Error(String),
}

/// One operation, on node `node`'s connection `conn`.
async fn operation(sh: &Shared, rng: &mut Rng, node: usize, conn: usize) -> (bool, Ended) {
    let ch = sh.conns[node][conn].clone();
    let a = &sh.args;
    let l = &sh.load;
    let nparts = a.parts.len() as u64;
    let spanning = l.span_share > 0.0 && rng.chance(l.span_share);
    let home = rng.below(nparts) as usize;
    let hot = (spanning || l.single == Single::Txn) && l.conflict > 0.0 && rng.chance(l.conflict);
    let row_in = |rng: &mut Rng, p: usize, hot: bool| {
        let n = if hot {
            rng.below(a.hot_keys)
        } else {
            a.hot_keys + rng.below(a.rows.saturating_sub(a.hot_keys).max(1))
        };
        row_key(&a.parts[p], n)
    };
    let home_row = row_in(rng, home, hot);
    if !spanning && l.single == Single::Mutate {
        let r = DataClient::new(ch)
            .mutate(proto::MutateRequest {
                writes: Some(put_row(&a.table, home_row, value(a.value_bytes))),
                durability: a.durability.proto() as i32,
            })
            .await;
        return (
            false,
            ended(r.map(|_| 0), &sh.conns[node][conn], None).await,
        );
    }
    let mut t = TransactionsClient::new(ch.clone());
    let begun = t
        .begin(proto::BeginRequest {
            durability: a.durability.proto() as i32,
            route: Some(proto::RoutingKey {
                key: Some(proto::routing_key::Key::Row(proto::RowKey {
                    table: a.table.clone(),
                    row: home_row.clone().into(),
                })),
            }),
            spans_partitions: spanning,
        })
        .await;
    let id = match begun {
        Ok(r) => r.into_inner().txn_id,
        Err(e) => return (spanning, ended(Err(e), &ch, None).await),
    };
    if hot {
        let read = t
            .get(proto::TxnGetRequest {
                txn_id: id.clone(),
                table: a.table.clone(),
                row: home_row.clone().into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
            })
            .await;
        if let Err(e) = read {
            return (spanning, ended(Err(e), &ch, Some(&id)).await);
        }
    }
    let mut rows = vec![put_row(&a.table, home_row, value(a.value_bytes))];
    if spanning {
        // The other partitions, from the one after the home, wrapping.
        for k in 1..l.span_parts {
            let p = (home + k) % a.parts.len();
            rows.push(put_row(
                &a.table,
                row_in(rng, p, false),
                value(a.value_bytes),
            ));
        }
    }
    let r = t
        .commit(proto::TxnCommitRequest {
            txn_id: id.clone(),
            rows,
        })
        .await
        .map(|r| r.into_inner().parts.iter().filter(|p| !p.applied).count() as u64);
    (spanning, ended(r, &ch, Some(&id)).await)
}

/// Classifies a reply; an `OutcomeUnknown` on a transaction asks `Outcome`.
async fn ended(
    r: Result<u64, tonic::Status>,
    ch: &tonic::transport::Channel,
    txn: Option<&str>,
) -> Ended {
    let e = match r {
        Ok(unapplied) => return Ended::Committed { unapplied },
        Err(e) => e,
    };
    let code = api_code(&e);
    match code.as_str() {
        "Conflict" => Ended::Conflict,
        "OutcomeUnknown" => {
            let Some(id) = txn else {
                return Ended::Unknown(2);
            };
            // The decision may still be on its way: ask for up to 10 s.
            let mut t = TransactionsClient::new(ch.clone());
            for _ in 0..100 {
                if let Ok(o) = t
                    .outcome(proto::TxnOutcomeRequest { txn_id: id.into() })
                    .await
                {
                    match o.into_inner().state() {
                        proto::TxnOutcomeState::Committed => return Ended::Unknown(0),
                        proto::TxnOutcomeState::Aborted => return Ended::Unknown(1),
                        proto::TxnOutcomeState::Unknown => return Ended::Unknown(2),
                        _ => {}
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ended::Unknown(2)
        }
        _ => Ended::Error(code),
    }
}

fn record(t: &mut Tally, spanning: bool, us: u64, e: Ended) {
    let k = if spanning {
        &mut t.spanning
    } else {
        &mut t.single
    };
    match e {
        Ended::Committed { unapplied } => {
            k.lat.record(us);
            k.committed += 1;
            k.unapplied += unapplied;
        }
        Ended::Conflict => k.conflict += 1,
        Ended::Unknown(i) => k.unknown[i] += 1,
        Ended::Error(code) => *k.errors.entry(code).or_default() += 1,
    }
}

/// A closed-loop caller.
async fn caller(sh: Arc<Shared>, i: usize) -> Tally {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ ((i as u64 + 1) * 0x2545_f491_4f6c_dd1d));
    let node = i % sh.conns.len();
    let conn = (i / sh.conns.len()) % sh.conns[node].len();
    let mut t = Tally::new();
    while !sh.stop.load(Ordering::Relaxed) {
        let start = Instant::now();
        let (spanning, e) = operation(&sh, &mut rng, node, conn).await;
        if sh.measuring.load(Ordering::Relaxed) {
            record(&mut t, spanning, start.elapsed().as_micros() as u64, e);
        }
    }
    t
}

/// An open-loop caller: takes due times from the pacer, and times each operation from its
/// due time, so a server that falls behind is measured, not hidden.
async fn paced_caller(
    sh: Arc<Shared>,
    i: usize,
    mut due: tokio::sync::mpsc::UnboundedReceiver<Instant>,
    outstanding: Arc<AtomicU64>,
) -> Tally {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ ((i as u64 + 1) * 0x2545_f491_4f6c_dd1d));
    let node = i % sh.conns.len();
    let conn = (i / sh.conns.len()) % sh.conns[node].len();
    let mut t = Tally::new();
    while let Some(at) = due.recv().await {
        // Past the measured phase, what's still queued is dropped, not sent: a server that
        // fell behind would otherwise keep working through it after the counters' last read.
        if sh.stop.load(Ordering::Relaxed) {
            outstanding.fetch_sub(1, Ordering::Relaxed);
            continue;
        }
        let (spanning, e) = operation(&sh, &mut rng, node, conn).await;
        if sh.measuring.load(Ordering::Relaxed) {
            record(&mut t, spanning, at.elapsed().as_micros() as u64, e);
        }
        outstanding.fetch_sub(1, Ordering::Relaxed);
    }
    t
}

/// Hands due times `started + k / rate` to the caller with the fewest outstanding, until
/// `stop`; returns the operations skipped because the pacer itself was late.
fn pace(
    rate: f64,
    stop: Arc<AtomicBool>,
    callers: Vec<(tokio::sync::mpsc::UnboundedSender<Instant>, Arc<AtomicU64>)>,
) -> u64 {
    const SPIN: Duration = Duration::from_micros(200);
    let started = Instant::now();
    let due = |k: u64| started + Duration::from_secs_f64(k as f64 / rate.max(1e-9));
    let mut skipped = 0;
    let mut k = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let at = due(k);
        let now = Instant::now();
        if at > now + SPIN {
            std::thread::sleep(at - now - SPIN);
        }
        while Instant::now() < at {
            std::hint::spin_loop();
        }
        if Instant::now() >= due(k + 1) {
            skipped += 1;
        } else {
            let n = callers.len();
            let (tx, w) = (0..n)
                .map(|i| &callers[(k as usize + i) % n])
                .min_by_key(|(_, w)| w.load(Ordering::Relaxed))
                .expect("a caller");
            w.fetch_add(1, Ordering::Relaxed);
            if tx.send(at).is_err() {
                w.fetch_sub(1, Ordering::Relaxed);
            }
        }
        k += 1;
    }
    skipped
}

/// The external-share counters and in-doubt gauges of one node's `/metrics`, summed over
/// its partitions.
#[derive(Clone, Copy, Default, Debug)]
struct External {
    prepares: u64,
    commits: u64,
    aborts: u64,
    holds: u64,
    evicted: u64,
    in_doubt: u64,
    in_doubt_bytes: u64,
}

async fn external(ch: &tonic::transport::Channel) -> Result<External, tonic::Status> {
    let text = proto::admin_client::AdminClient::new(ch.clone())
        .metrics(proto::Empty {})
        .await?
        .into_inner()
        .text;
    let mut e = External::default();
    for l in text.lines().filter(|l| !l.starts_with('#')) {
        let Some((series, v)) = l.rsplit_once(' ') else {
            continue;
        };
        let v: u64 = v.parse::<f64>().map_or(0, |f| f as u64);
        let name = series.split('{').next().unwrap_or(series);
        match name {
            "pigeonhole_txn_external_prepares_total" => e.prepares += v,
            "pigeonhole_txn_external_decides_total" if series.contains("\"committed\"") => {
                e.commits += v
            }
            "pigeonhole_txn_external_decides_total" if series.contains("\"aborted\"") => {
                e.aborts += v
            }
            "pigeonhole_feed_external_holds_total" => e.holds += v,
            "pigeonhole_feed_external_evictions_total" => e.evicted += v,
            "pigeonhole_cluster_txn_in_doubt_shares" => e.in_doubt += v,
            "pigeonhole_cluster_txn_in_doubt_wal_bytes" => e.in_doubt_bytes += v,
            _ => {}
        }
    }
    Ok(e)
}

/// Every node's [`External`], summed; a node that doesn't answer (stopped) counts nothing.
async fn externals(sh: &Shared) -> External {
    let mut sum = External::default();
    for node in &sh.conns {
        let read = tokio::time::timeout(Duration::from_secs(1), external(&node[0])).await;
        if let Ok(Ok(e)) = read {
            sum.prepares += e.prepares;
            sum.commits += e.commits;
            sum.aborts += e.aborts;
            sum.holds += e.holds;
            sum.evicted += e.evicted;
            sum.in_doubt += e.in_doubt;
            sum.in_doubt_bytes += e.in_doubt_bytes;
        }
    }
    sum
}

/// What the subscribers saw.
struct FeedSeen {
    events: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    lag: Arc<Mutex<Hist>>,
}

/// Starts `n` subscriptions to the table, spread over the nodes; the first decodes events
/// for the lag.
async fn subscribe(sh: &Arc<Shared>, n: usize) -> Result<FeedSeen, Box<dyn std::error::Error>> {
    let seen = FeedSeen {
        events: Arc::new(AtomicU64::new(0)),
        dropped: Arc::new(AtomicU64::new(0)),
        lag: Arc::new(Mutex::new(Hist::new())),
    };
    for i in 0..n {
        let ch = sh.conns[i % sh.conns.len()][0].clone();
        let request = proto::SubscribeRequest {
            tables: vec![sh.args.table.clone()],
            ..Default::default()
        };
        let (events, dropped, lag, sh2) = (
            Arc::clone(&seen.events),
            Arc::clone(&seen.dropped),
            Arc::clone(&seen.lag),
            Arc::clone(sh),
        );
        if i == 0 {
            let mut stream = FeedClient::new(ch).subscribe(request).await?.into_inner();
            tokio::spawn(async move {
                while !sh2.stop.load(Ordering::Relaxed) {
                    let Ok(Some(e)) = stream.message().await else {
                        if !sh2.stop.load(Ordering::Relaxed) {
                            dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    };
                    if !sh2.measuring.load(Ordering::Relaxed) {
                        continue;
                    }
                    events.fetch_add(1, Ordering::Relaxed);
                    if let Some(proto::feed_event::Event::Commit(c)) = e.event {
                        let now = now_nanos();
                        let mut h = lag.lock().unwrap_or_else(|p| p.into_inner());
                        for chg in &c.changes {
                            if let Some(proto::Value {
                                kind: Some(proto::value::Kind::Bytes(b)),
                            }) = &chg.value
                                && let Some(sent) = b.get(..8)
                            {
                                let sent = u64::from_le_bytes(sent.try_into().unwrap_or_default());
                                h.record(now.saturating_sub(sent) / 1000);
                            }
                        }
                    }
                }
            });
        } else {
            let mut stream = count_frames(ch, request).await?;
            tokio::spawn(async move {
                while !sh2.stop.load(Ordering::Relaxed) {
                    let Ok(Some(_)) = stream.message().await else {
                        if !sh2.stop.load(Ordering::Relaxed) {
                            dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    };
                    if sh2.measuring.load(Ordering::Relaxed) {
                        events.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    }
    Ok(seen)
}

/// `Feed.Subscribe` with a codec that counts each event's frame and decodes nothing.
async fn count_frames(
    conn: tonic::transport::Channel,
    request: proto::SubscribeRequest,
) -> Result<tonic::Streaming<usize>, Box<dyn std::error::Error>> {
    let mut grpc = tonic::client::Grpc::new(conn);
    grpc.ready().await?;
    let path =
        tonic::codegen::http::uri::PathAndQuery::from_static("/pigeonhole.v1.Feed/Subscribe");
    Ok(grpc
        .server_streaming(tonic::Request::new(request), path, CountCodec)
        .await?
        .into_inner())
}

/// Encodes a `SubscribeRequest`; decodes each message as its length only.
struct CountCodec;

impl tonic::codec::Codec for CountCodec {
    type Encode = proto::SubscribeRequest;
    type Decode = usize;
    type Encoder = CountCodec;
    type Decoder = CountCodec;

    fn encoder(&mut self) -> Self::Encoder {
        CountCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        CountCodec
    }
}

impl tonic::codec::Encoder for CountCodec {
    type Item = proto::SubscribeRequest;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        use prost::Message;
        item.encode(dst)
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl tonic::codec::Decoder for CountCodec {
    type Item = usize;
    type Error = tonic::Status;

    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        use prost::bytes::Buf;
        let n = src.remaining();
        src.advance(n);
        Ok(Some(n))
    }
}

fn stall(pid: u32, signal: &str) {
    let ok = std::process::Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success());
    println!(
        "stall: kill {signal} {pid} at {:.1}s: {}",
        epoch().elapsed().as_secs_f64(),
        if ok { "ok" } else { "FAILED" }
    );
}

async fn run(args: Args, load: Load) -> Result<(), Box<dyn std::error::Error>> {
    epoch();
    let mut conns = Vec::new();
    for url in &args.servers {
        let mut node = Vec::new();
        for _ in 0..args.conns.max(1) {
            node.push(connect(url).await?);
        }
        conns.push(node);
    }
    let sh = Arc::new(Shared {
        args: args.clone(),
        load: load.clone(),
        conns,
        measuring: AtomicBool::new(false),
        stop: AtomicBool::new(false),
    });
    println!(
        "shape {}: {} callers, {}, {}% spanning x{}, {}% conflicts on {} hot rows per \
         partition, single as {:?}, {} subscribers, {} nodes, {} partitions, durability {:?}",
        args.shape
            .map_or_else(|| "custom".to_owned(), |s| format!("{s:?}")),
        load.writers,
        load.rate.map_or_else(
            || "closed loop".to_owned(),
            |r| format!("{r:.0} ops/s paced")
        ),
        load.span_share,
        load.span_parts,
        load.conflict,
        args.hot_keys,
        load.single,
        load.subscribers,
        args.servers.len(),
        args.parts.len(),
        args.durability,
    );
    let seen = subscribe(&sh, load.subscribers).await?;
    // The callers: closed loop, or fed by the pacer.
    let mut tasks = Vec::new();
    let mut pacer = None;
    let pace_stop = Arc::new(AtomicBool::new(false));
    if let Some(rate) = load.rate {
        let mut feeds = Vec::new();
        for i in 0..load.writers {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let w = Arc::new(AtomicU64::new(0));
            feeds.push((tx, Arc::clone(&w)));
            tasks.push(tokio::spawn(paced_caller(Arc::clone(&sh), i, rx, w)));
        }
        let stop = Arc::clone(&pace_stop);
        pacer = Some(std::thread::spawn(move || pace(rate, stop, feeds)));
    } else {
        for i in 0..load.writers {
            tasks.push(tokio::spawn(caller(Arc::clone(&sh), i)));
        }
    }
    tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
    // The measured phase.
    let before = externals(&sh).await;
    let cpu_before: Vec<Option<f64>> = args.server_pids.iter().map(|&p| cpu_seconds(p)).collect();
    sh.measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let end = started + Duration::from_secs(args.seconds);
    let mut peak = External::default();
    let mut stalled = false;
    let mut resumed = false;
    while Instant::now() < end {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let secs = started.elapsed().as_secs();
        if let (Some(pid), Some(at), Some(dur)) = (args.stall_pid, args.stall_at, args.stall_for) {
            if !stalled && secs >= at {
                stall(pid, "-STOP");
                stalled = true;
            }
            if stalled && !resumed && secs >= at + dur {
                stall(pid, "-CONT");
                resumed = true;
            }
        }
        let now = externals(&sh).await;
        peak.in_doubt = peak.in_doubt.max(now.in_doubt);
        peak.in_doubt_bytes = peak.in_doubt_bytes.max(now.in_doubt_bytes);
    }
    // A stall that hadn't ended by the phase's end ends now, before anything else.
    if stalled
        && !resumed
        && let Some(pid) = args.stall_pid
    {
        stall(pid, "-CONT");
    }
    sh.measuring.store(false, Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    let cpu: f64 = args
        .server_pids
        .iter()
        .zip(&cpu_before)
        .filter_map(|(&p, b)| Some(cpu_seconds(p)? - (*b)?))
        .sum();
    sh.stop.store(true, Ordering::Relaxed);
    pace_stop.store(true, Ordering::Relaxed);
    let skipped = pacer.map_or(0, |p| p.join().unwrap_or(0));
    let mut tally = Tally::new();
    for t in tasks {
        if let Ok(t) = tokio::time::timeout(Duration::from_secs(30), t).await {
            tally.merge(&t?);
        }
    }
    // The counters once the last operations' decides have had a moment.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = externals(&sh).await;
    report(
        &args, &load, &tally, secs, cpu, skipped, &seen, before, after, peak,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn report(
    args: &Args,
    load: &Load,
    t: &Tally,
    secs: f64,
    cpu: f64,
    skipped: u64,
    seen: &FeedSeen,
    before: External,
    after: External,
    peak: External,
) {
    let mut all = t.single.clone();
    all.merge(&t.spanning);
    println!(
        "{:<9} {:>9} {:>8} {:>8} {:>8} {:>9} {:>9} {:>8} {:>7} {:>9}",
        "kind",
        "committed",
        "p50 us",
        "p99 us",
        "p999 us",
        "conflict",
        "unknown",
        "other",
        "ops/s",
        "unapplied"
    );
    for (name, k) in [
        ("single", &t.single),
        ("spanning", &t.spanning),
        ("all", &all),
    ] {
        println!(
            "{:<9} {:>9} {:>8} {:>8} {:>8} {:>9} {:>9} {:>8} {:>7.0} {:>9}",
            name,
            k.committed,
            k.lat.quantile(0.5),
            k.lat.quantile(0.99),
            k.lat.quantile(0.999),
            k.conflict,
            k.unknowns(),
            k.other(),
            k.committed as f64 / secs,
            k.unapplied
        );
    }
    for (name, k) in [("single", &t.single), ("spanning", &t.spanning)] {
        if k.unknowns() > 0 {
            println!(
                "  {name} OutcomeUnknown, then Outcome: committed {}, aborted {}, unknown {}",
                k.unknown[0], k.unknown[1], k.unknown[2]
            );
        }
        if !k.errors.is_empty() {
            println!("  {name} errors: {:?}", k.errors);
        }
    }
    let lag = seen.lag.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let events = seen.events.load(Ordering::Relaxed);
    let per_sub = if load.subscribers > 0 {
        events as f64 / load.subscribers as f64
    } else {
        0.0
    };
    if load.subscribers > 0 {
        println!(
            "feed: {:.0} events per subscriber, lag p50 {} us, p99 {} us, dropped streams {}",
            per_sub,
            lag.quantile(0.5),
            lag.quantile(0.99),
            seen.dropped.load(Ordering::Relaxed)
        );
    }
    let d = |a: u64, b: u64| a.saturating_sub(b);
    let prepares = d(after.prepares, before.prepares);
    println!(
        "external (ICR 0148, all nodes): prepares {}, decides committed {} aborted {}, feed \
         holds {} evicted {}; in doubt at the end {} ({} bytes), peak {} ({} bytes)",
        prepares,
        d(after.commits, before.commits),
        d(after.aborts, before.aborts),
        d(after.holds, before.holds),
        d(after.evicted, before.evicted),
        after.in_doubt,
        after.in_doubt_bytes,
        peak.in_doubt,
        peak.in_doubt_bytes
    );
    if skipped > 0 {
        println!("pacer: {skipped} operations skipped (the pacer itself was late)");
    }
    if load.span_share > 0.0 && t.spanning.committed > 0 && prepares == 0 {
        println!(
            "FUNCTIONAL FAILED: {} spanning commits but no external prepare counted",
            t.spanning.committed
        );
    }
    let cpu_per_op = if all.committed > 0 && cpu > 0.0 {
        cpu * 1e6 / all.committed as f64
    } else {
        0.0
    };
    println!(
        "RESULT label={} shape={} writers={} rate={} secs={:.1} ops_per_s={:.0} \
         single_committed={} single_p50_us={} single_p99_us={} single_p999_us={} \
         span_committed={} span_p50_us={} span_p99_us={} span_p999_us={} conflicts={} \
         unknown={} other={} unapplied={} cpu_us_per_op={:.1} events_per_sub={:.0} \
         lag_p50_us={} lag_p99_us={} dropped={} ext_prepares={} ext_commits={} ext_aborts={} \
         feed_holds={} feed_evicted={} in_doubt_peak={} in_doubt_end={} skipped={}",
        if args.label.is_empty() {
            "-"
        } else {
            &args.label
        },
        args.shape
            .map_or_else(|| "custom".to_owned(), |s| format!("{s:?}").to_lowercase()),
        load.writers,
        load.rate.unwrap_or(0.0),
        secs,
        all.committed as f64 / secs,
        t.single.committed,
        t.single.lat.quantile(0.5),
        t.single.lat.quantile(0.99),
        t.single.lat.quantile(0.999),
        t.spanning.committed,
        t.spanning.lat.quantile(0.5),
        t.spanning.lat.quantile(0.99),
        t.spanning.lat.quantile(0.999),
        all.conflict,
        all.unknowns(),
        all.other(),
        all.unapplied,
        cpu_per_op,
        per_sub,
        lag.quantile(0.5),
        lag.quantile(0.99),
        seen.dropped.load(Ordering::Relaxed),
        prepares,
        d(after.commits, before.commits),
        d(after.aborts, before.aborts),
        d(after.holds, before.holds),
        d(after.evicted, before.evicted),
        peak.in_doubt,
        after.in_doubt,
        skipped
    );
}
