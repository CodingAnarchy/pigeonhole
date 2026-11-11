//! The serve bench (D313 ticket 9b): a mixed workload against `phdb serve`, over gRPC and
//! HTTP/JSON, for latency per operation and the server's CPU per operation. R1's acceptance
//! run compares `--serve-arg=--affinity --serve-arg=off` against the default.
//!
//! It creates a database with a table `t` of `--rows` rows (`u00000000`..), one cell of
//! `--value-bytes` each, starts `phdb serve` on it as its own process, and runs each mix for
//! `--secs` after `--warmup-secs` (discarded), with `--clients` callers. Over gRPC they share
//! `--conns` connections; over HTTP/JSON each caller is a thread with its own keep-alive
//! connection. Per transport and mix it prints the operations per second, the server's CPU
//! per operation (Linux, from `/proc`), how many calls were refused (`Overloaded`), and each
//! operation's p50, p99 and p99.9.
//!
//! The mixes:
//! - `get`, `readrow`, `mutate`: one point operation, on uniformly chosen rows;
//! - `batch`: a `Batch` of 10 rows' puts;
//! - `scan`: a scan of 10 rows from a uniformly chosen row;
//! - `ycsb-a` (50% `Get`, 50% `Mutate`), `ycsb-b` (95%, 5%) and `ycsb-c` (100% `Get`), on
//!   rows chosen by a Zipfian distribution (θ 0.99, YCSB's), hot rows scattered.
//!
//! Writes use the server's durability default (`GroupSync`), or `--durability` per request.
//!
//! **Against a running server** (D317 8b's cluster runs): `--server URL` skips the build and
//! the spawn, and `--server-pid` names the process whose CPU to read. `--load` writes the
//! rows through the server first (`Mutate`s from `--clients` callers), so a cluster's router
//! places each by its split keys. The table `t` (family `f`) is created if it's missing and
//! the server serves `Schema.CreateTable`; otherwise it must exist. The load visits the rows
//! in a scattered order, so a cluster's partitions fill together, at `--load-durability`
//! (`Buffered` by default). A row refused (`Overloaded`) or left unknown for 30 s stops it,
//! and so does any other error, with the server's message.
//!
//! On a cluster, a `Batch` whose rows straddle a split key is refused as `CrossPartition`;
//! such calls are counted on the row's line (`cross-partition N`), not timed.
//!
//! **Large stores:** row keys are 8 digits, so up to 100M rows. About 20M of 100 B load in a
//! few minutes either way. The Zipfian setup sums 20M terms once, about a second.
//!
//! ```text
//! cargo build --release -p pigeonhole-cli --features serve --bin phdb --example serve_load
//! target/release/examples/serve_load --phdb target/release/phdb --dir /tmp/serveload
//! # R1's acceptance: the same, with the server's threads unmasked
//! target/release/examples/serve_load --phdb target/release/phdb --dir /tmp/serveload \
//!     --serve-arg=--affinity --serve-arg=off
//! # D317 8b: a cluster's entry node, loaded through it, Buffered writes
//! target/release/examples/serve_load --server http://10.0.0.2:7448 --server-pid 4242 \
//!     --load --rows 20000000 --durability buffered
//! ```

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use pigeonhole::{Durability, Family, Options, Pigeonhole};

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
mod proto {
    include!("../src/serve/proto/pigeonhole.v1.rs");
}

use proto::data_client::DataClient;

#[derive(Parser, Debug, Clone)]
struct Args {
    /// The `phdb` binary to serve with (built with `--features serve`).
    #[arg(long, required_unless_present = "server")]
    phdb: Option<PathBuf>,
    /// A directory for the database (made if missing; the database is built once and reused).
    #[arg(long, required_unless_present = "server")]
    dir: Option<PathBuf>,
    /// A running server to load instead of starting one, as `http://HOST:PORT`: gRPC and
    /// HTTP/JSON share the port.
    #[arg(long, value_name = "URL", conflicts_with_all = ["phdb", "dir", "serve_args", "server_cpus", "shards"])]
    server: Option<String>,
    /// The running server's process id, for its CPU per operation (Linux, same host).
    #[arg(long, value_name = "PID", requires = "server")]
    server_pid: Option<u32>,
    /// Write the `--rows` rows through the running server before the mixes.
    #[arg(long, requires = "server")]
    load: bool,
    /// Each write's durability (`Mutate` and `Batch`, and `--load`'s); `default` leaves it to
    /// the server's writer default.
    #[arg(long, value_enum, default_value_t = WriteDurability::Default)]
    durability: WriteDurability,
    /// `--load`'s writes' durability. What it loads doesn't depend on it, so the default is
    /// the fastest that still lands (perf2's N1 on #1712).
    #[arg(long, value_enum, default_value_t = WriteDurability::Buffered)]
    load_durability: WriteDurability,
    /// The port of the server this process starts (ignored with `--server`).
    #[arg(long, default_value_t = 7462)]
    port: u16,
    /// The shard count the database is built and served at (default: `phdb serve`'s, half
    /// the physical cores, not the library's one per core). It's passed to `phdb serve` as
    /// `--shards`, since the count isn't stored in the file (ICR 0132); a `--shards` among
    /// the `--serve-arg`s wins.
    #[arg(long)]
    shards: Option<usize>,
    /// Rows in the table.
    #[arg(long, default_value_t = 100_000)]
    rows: u64,
    /// Bytes in each row's cell, and in each write's.
    #[arg(long, default_value_t = 100)]
    value_bytes: usize,
    /// Measured seconds per transport and mix.
    #[arg(long, default_value_t = 10)]
    secs: u64,
    /// Seconds of the same load before each measurement, discarded.
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    /// Concurrent callers.
    #[arg(long, default_value_t = 64)]
    clients: usize,
    /// gRPC connections the callers share (HTTP/JSON callers have one each).
    #[arg(long, default_value_t = 8)]
    conns: usize,
    /// Transports to run.
    #[arg(long, value_enum, default_values_t = [Transport::Grpc, Transport::Http])]
    transport: Vec<Transport>,
    /// Mixes to run (all by default).
    #[arg(long, value_enum)]
    mix: Vec<Mix>,
    /// Run `phdb serve` on these CPUs, through `taskset -c` (Linux).
    #[arg(long, value_name = "LIST")]
    server_cpus: Option<String>,
    /// Run this process on these CPUs, e.g. `0-11` (Linux).
    #[arg(long, value_name = "LIST")]
    client_cpus: Option<String>,
    /// An argument passed through to `phdb serve`, repeatable (for example
    /// `--serve-arg=--affinity --serve-arg=off`).
    #[arg(long = "serve-arg", allow_hyphen_values = true)]
    serve_args: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum WriteDurability {
    Default,
    None,
    Buffered,
    GroupSync,
    Sync,
}

impl WriteDurability {
    fn proto(self) -> proto::Durability {
        match self {
            WriteDurability::Default => proto::Durability::Unspecified,
            WriteDurability::None => proto::Durability::None,
            WriteDurability::Buffered => proto::Durability::Buffered,
            WriteDurability::GroupSync => proto::Durability::GroupSync,
            WriteDurability::Sync => proto::Durability::Sync,
        }
    }

    /// The JSON field for a write's body, or nothing for the default.
    fn json(self) -> &'static str {
        match self {
            WriteDurability::Default => "",
            WriteDurability::None => r#","durability":"DURABILITY_NONE""#,
            WriteDurability::Buffered => r#","durability":"DURABILITY_BUFFERED""#,
            WriteDurability::GroupSync => r#","durability":"DURABILITY_GROUP_SYNC""#,
            WriteDurability::Sync => r#","durability":"DURABILITY_SYNC""#,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Transport {
    Grpc,
    Http,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mix {
    Get,
    Readrow,
    Mutate,
    Batch,
    Scan,
    YcsbA,
    YcsbB,
    YcsbC,
}

const MIXES: [Mix; 8] = [
    Mix::Get,
    Mix::Readrow,
    Mix::Mutate,
    Mix::Batch,
    Mix::Scan,
    Mix::YcsbA,
    Mix::YcsbB,
    Mix::YcsbC,
];

/// The operations, as each mix draws them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Get,
    ReadRow,
    Mutate,
    Batch,
    Scan,
}

const OPS: [Op; 5] = [Op::Get, Op::ReadRow, Op::Mutate, Op::Batch, Op::Scan];

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Get => "Get",
            Op::ReadRow => "ReadRow",
            Op::Mutate => "Mutate",
            Op::Batch => "Batch",
            Op::Scan => "Scan",
        }
    }
}

/// Rows a `Batch` writes, and a scan reads.
const BATCH_ROWS: u64 = 10;
const SCAN_ROWS: u64 = 10;

impl Mix {
    fn name(self) -> &'static str {
        match self {
            Mix::Get => "get",
            Mix::Readrow => "readrow",
            Mix::Mutate => "mutate",
            Mix::Batch => "batch",
            Mix::Scan => "scan",
            Mix::YcsbA => "ycsb-a",
            Mix::YcsbB => "ycsb-b",
            Mix::YcsbC => "ycsb-c",
        }
    }

    /// The next operation, from a uniform draw in `0..100`.
    fn op(self, pct: u64) -> Op {
        match self {
            Mix::Get | Mix::YcsbC => Op::Get,
            Mix::Readrow => Op::ReadRow,
            Mix::Mutate => Op::Mutate,
            Mix::Batch => Op::Batch,
            Mix::Scan => Op::Scan,
            Mix::YcsbA if pct < 50 => Op::Get,
            Mix::YcsbB if pct < 95 => Op::Get,
            Mix::YcsbA | Mix::YcsbB => Op::Mutate,
        }
    }

    fn zipfian(self) -> bool {
        matches!(self, Mix::YcsbA | Mix::YcsbB | Mix::YcsbC)
    }
}

/// splitmix64: each caller's own stream, seeded by its index.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// YCSB's Zipfian generator (Gray et al.), θ 0.99, over `0..n`; the rank is scattered over
/// the key space by a hash, as YCSB's scrambled Zipfian does, so hot rows aren't neighbours.
struct Zipf {
    n: f64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipf {
    fn new(n: u64) -> Self {
        let theta = 0.99;
        let zeta = |n: u64| (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let zetan = zeta(n);
        let zeta2 = zeta(2.min(n));
        let nf = n as f64;
        Self {
            n: nf,
            theta,
            alpha: 1.0 / (1.0 - theta),
            zetan,
            eta: (1.0 - (2.0 / nf).powf(1.0 - theta)) / (1.0 - zeta2 / zetan),
        }
    }

    fn rank(&self, rng: &mut Rng) -> u64 {
        let u = rng.unit();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        ((self.n * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64).min(self.n as u64 - 1)
    }
}

/// The row a draw lands on.
struct Keys {
    rows: u64,
    zipf: Option<Arc<Zipf>>,
}

impl Keys {
    fn row(&self, rng: &mut Rng) -> u64 {
        match &self.zipf {
            Some(z) => {
                let mut s = Rng(z.rank(rng));
                s.next() % self.rows
            }
            None => rng.next() % self.rows,
        }
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("u{i:08}").into_bytes()
}

/// The shard count to build and serve at: `--shards`, else `phdb serve`'s default
/// (`placement::default_shards`, half the physical cores).
fn shards(args: &Args) -> usize {
    args.shards
        .unwrap_or_else(|| (num_cpus::get_physical() / 2).max(1))
}

/// The database in `--dir`, built at `shards` if missing. One that exists is served as it
/// is: the count isn't stored in the file (ICR 0132), so `phdb serve` gets it as `--shards`
/// rather than this process opening the file to check (perf287's F1 on #1697).
fn build(args: &Args, shards: usize) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = args
        .dir
        .as_ref()
        .ok_or("--dir is needed without --server")?;
    std::fs::create_dir_all(dir)?;
    let file = dir.join("serveload.phdb");
    if file.exists() {
        return Ok(file);
    }
    let db = Pigeonhole::open(
        &file,
        Options::default().create_if_missing(true).shards(shards),
    )?;
    let t = db.table("t")?.family("f", Family::default()).create()?;
    let value = vec![b'v'; args.value_bytes];
    let mut batch = db.write_batch();
    for i in 0..args.rows {
        batch.put(&t, &key(i), "f", b"q", &value);
        if batch.len() >= 4096 {
            std::mem::replace(&mut batch, db.write_batch()).commit_with(Durability::None)?;
        }
    }
    batch.commit_with(Durability::None)?;
    db.flush()?;
    db.close()?;
    Ok(file)
}

struct Server(Child);

impl Server {
    fn start(args: &Args, file: &Path, shards: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let phdb = args
            .phdb
            .as_ref()
            .ok_or("--phdb is needed without --server")?;
        let mut cmd = match &args.server_cpus {
            Some(list) => {
                let mut c = Command::new("taskset");
                c.arg("-c").arg(list).arg(phdb);
                c
            }
            None => Command::new(phdb),
        };
        let child = cmd
            .arg("serve")
            .arg(file)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", args.port))
            .args(
                (!args
                    .serve_args
                    .iter()
                    .any(|a| a == "--shards" || a.starts_with("--shards=")))
                .then(|| ["--shards".to_owned(), shards.to_string()])
                .into_iter()
                .flatten(),
            )
            .args(&args.serve_args)
            .spawn()?;
        Ok(Self(child))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Latencies per operation, and the refused calls.
#[derive(Default)]
struct Tally {
    lat: [Vec<Duration>; OPS.len()],
    refused: u64,
    /// Calls a cluster refused as `CrossPartition`: a `Batch` whose rows straddle a split
    /// key (or wrap past the last row). Counted, not timed (perf2's F2, feed1248's F1 on #1712).
    cross: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        for (a, b) in self.lat.iter_mut().zip(other.lat) {
            a.extend(b);
        }
        self.refused += other.refused;
        self.cross += other.cross;
    }

    fn ops(&self) -> usize {
        self.lat.iter().map(Vec::len).sum()
    }
}

/// One caller's state.
struct Caller {
    rng: Rng,
    mix: Mix,
    keys: Arc<Keys>,
    value: Arc<Vec<u8>>,
}

impl Caller {
    fn next(&mut self) -> (Op, u64) {
        let op = self.mix.op(self.rng.next() % 100);
        let row = self.keys.row(&mut self.rng);
        (op, row)
    }
}

fn put_write(value: &[u8]) -> proto::Write {
    proto::Write {
        op: Some(proto::write::Op::Put(proto::Put {
            family: "f".into(),
            qualifier: b"q".to_vec().into(),
            timestamp: None,
            value: Some(proto::Value {
                kind: Some(proto::value::Kind::Bytes(value.to_vec().into())),
            }),
        })),
    }
}

fn row_writes(row: u64, value: &[u8]) -> proto::RowWrites {
    proto::RowWrites {
        row: Some(proto::RowKey {
            table: "t".into(),
            row: key(row).into(),
        }),
        writes: vec![put_write(value)],
    }
}

/// How a call ended, short of an error that stops the run.
#[derive(Debug)]
enum Outcome {
    Served,
    /// `Overloaded`: shed by the server, nothing done.
    Refused,
    /// `CrossPartition`: a cluster's batch across partitions, nothing done.
    Cross,
}

/// The stable code (ICR 0112) a gRPC status carries in its `ErrorDetail`, or "".
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
        .map_or_else(String::new, |d| d.code)
}

/// One gRPC call: served, refused (`Overloaded`), across partitions (`CrossPartition`), or
/// an error (anything else, `PartitionUnavailable` included).
async fn grpc_call(
    c: &mut DataClient<tonic::transport::Channel>,
    op: Op,
    row: u64,
    rows: u64,
    value: &[u8],
    durability: proto::Durability,
) -> Result<Outcome, tonic::Status> {
    let r = match op {
        Op::Get => c
            .get(proto::GetRequest {
                table: "t".into(),
                row: key(row).into(),
                family: "f".into(),
                qualifier: b"q".to_vec().into(),
                ..Default::default()
            })
            .await
            .map(|_| ()),
        Op::ReadRow => c
            .read_row(proto::ReadRowRequest {
                table: "t".into(),
                row: key(row).into(),
                ..Default::default()
            })
            .await
            .map(|_| ()),
        Op::Mutate => c
            .mutate(proto::MutateRequest {
                writes: Some(row_writes(row, value)),
                durability: durability as i32,
            })
            .await
            .map(|_| ()),
        Op::Batch => c
            .batch(proto::BatchRequest {
                rows: (0..BATCH_ROWS)
                    .map(|i| row_writes((row + i) % rows, value))
                    .collect(),
                durability: durability as i32,
            })
            .await
            .map(|_| ()),
        Op::Scan => match c
            .scan(proto::ScanRequest {
                table: "t".into(),
                range: Some(scan_range(row)),
                limit: SCAN_ROWS,
                ..Default::default()
            })
            .await
        {
            Ok(stream) => {
                let mut stream = stream.into_inner();
                loop {
                    match stream.message().await {
                        Ok(Some(_)) => {}
                        Ok(None) => break Ok(()),
                        Err(e) => break Err(e),
                    }
                }
            }
            Err(e) => Err(e),
        },
    };
    match r {
        Ok(()) => Ok(Outcome::Served),
        Err(s) => match api_code(&s).as_str() {
            "Overloaded" => Ok(Outcome::Refused),
            "CrossPartition" => Ok(Outcome::Cross),
            _ => Err(s),
        },
    }
}

fn scan_range(row: u64) -> proto::KeyRange {
    proto::KeyRange {
        range: Some(proto::key_range::Range::Bounds(proto::Bounds {
            start: Some(proto::Bound {
                key: key(row).into(),
                inclusive: true,
            }),
            end: None,
        })),
    }
}

/// Standard base64, for proto3 JSON's `bytes`.
fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// One call's path and JSON body over HTTP.
fn http_request(
    op: Op,
    row: u64,
    rows: u64,
    value: &str,
    durability: WriteDurability,
) -> (&'static str, String) {
    let d = durability.json();
    let k = |r: u64| b64(&key(r));
    let put = |r: u64| {
        format!(
            r#"{{"row":{{"table":"t","row":"{}"}},"writes":[{{"put":{{"family":"f","qualifier":"cQ==","value":{{"bytes":"{value}"}}}}}}]}}"#,
            k(r)
        )
    };
    match op {
        Op::Get => (
            "Get",
            format!(
                r#"{{"table":"t","row":"{}","family":"f","qualifier":"cQ=="}}"#,
                k(row)
            ),
        ),
        Op::ReadRow => ("ReadRow", format!(r#"{{"table":"t","row":"{}"}}"#, k(row))),
        Op::Mutate => ("Mutate", format!(r#"{{"writes":{}{d}}}"#, put(row))),
        Op::Batch => (
            "Batch",
            format!(
                r#"{{"rows":[{}]{d}}}"#,
                (0..BATCH_ROWS)
                    .map(|i| put((row + i) % rows))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
        Op::Scan => (
            "Scan",
            format!(
                r#"{{"table":"t","range":{{"bounds":{{"start":{{"key":"{}","inclusive":true}}}}}},"limit":"{SCAN_ROWS}"}}"#,
                k(row)
            ),
        ),
    }
}

/// One HTTP/JSON caller, on a thread of its own, until `stop`.
fn http_caller(
    url: String,
    mut caller: Caller,
    rows: u64,
    durability: WriteDurability,
    stop: Arc<AtomicBool>,
) -> Tally {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let value = b64(&caller.value);
    let mut tally = Tally::default();
    while !stop.load(Ordering::Relaxed) {
        let (op, row) = caller.next();
        let (rpc, body) = http_request(op, row, rows, &value, durability);
        let t = Instant::now();
        let mut resp = agent
            .post(format!("{url}/v1/Data/{rpc}"))
            .header("Content-Type", "application/json")
            .send(body)
            .unwrap_or_else(|e| panic!("{rpc}: {e}"));
        let status = resp.status().as_u16();
        let text = resp
            .body_mut()
            .read_to_string()
            .unwrap_or_else(|e| panic!("{rpc}: {e}"));
        // The JSON body's stable code tells `Overloaded` (503) from another 503, such as a
        // cluster's `PartitionUnavailable`, which stops the run.
        let code = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v["code"].as_str().map(str::to_owned))
            .unwrap_or_default();
        match (status, code.as_str()) {
            (200, _) => tally.lat[op as usize].push(t.elapsed()),
            (503, "Overloaded") => tally.refused += 1,
            (409, "CrossPartition") => tally.cross += 1,
            _ => panic!("{rpc}: {status} {text}"),
        }
    }
    tally
}

/// Runs `mix` over `transport` with every caller until `secs` have passed.
async fn run(
    args: &Args,
    channels: &[tonic::transport::Channel],
    transport: Transport,
    mix: Mix,
    secs: u64,
    seed: u64,
    zipf: &Arc<Zipf>,
) -> Result<Tally, Box<dyn std::error::Error>> {
    let keys = Arc::new(Keys {
        rows: args.rows,
        zipf: mix.zipfian().then(|| Arc::clone(zipf)),
    });
    let value = Arc::new(vec![b'w'; args.value_bytes]);
    let stop = Arc::new(AtomicBool::new(false));
    let callers = (0..args.clients).map(|i| Caller {
        rng: Rng(seed ^ (i as u64 + 1).wrapping_mul(0x2545_F491_4F6C_DD1D)),
        mix,
        keys: Arc::clone(&keys),
        value: Arc::clone(&value),
    });
    let mut tally = Tally::default();
    match transport {
        Transport::Grpc => {
            let mut tasks = Vec::new();
            for (i, mut caller) in callers.enumerate() {
                let mut c = DataClient::new(channels[i % channels.len()].clone());
                let stop = Arc::clone(&stop);
                let rows = args.rows;
                let durability = args.durability.proto();
                tasks.push(tokio::spawn(async move {
                    let mut tally = Tally::default();
                    while !stop.load(Ordering::Relaxed) {
                        let (op, row) = caller.next();
                        let t = Instant::now();
                        match grpc_call(&mut c, op, row, rows, &caller.value, durability).await {
                            Ok(Outcome::Served) => tally.lat[op as usize].push(t.elapsed()),
                            Ok(Outcome::Refused) => tally.refused += 1,
                            Ok(Outcome::Cross) => tally.cross += 1,
                            Err(s) => panic!("{}: {s}", op.name()),
                        }
                    }
                    tally
                }));
            }
            tokio::time::sleep(Duration::from_secs(secs)).await;
            stop.store(true, Ordering::Relaxed);
            for t in tasks {
                tally.add(t.await?);
            }
        }
        Transport::Http => {
            let url = base_url(args);
            let (rows, durability) = (args.rows, args.durability);
            let threads: Vec<_> = callers
                .map(|caller| {
                    let (url, stop) = (url.clone(), Arc::clone(&stop));
                    std::thread::spawn(move || http_caller(url, caller, rows, durability, stop))
                })
                .collect();
            tokio::time::sleep(Duration::from_secs(secs)).await;
            stop.store(true, Ordering::Relaxed);
            for t in threads {
                tally.add(t.join().map_err(|_| "an HTTP caller panicked")?);
            }
        }
    }
    Ok(tally)
}

async fn connect(url: &str) -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match tonic::transport::Channel::from_shared(url.to_owned())?
            .connect()
            .await
        {
            Ok(c) => return Ok(c),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// A process's user and system CPU, in seconds (Linux); `None` elsewhere.
fn cpu_seconds(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the command's closing parenthesis: utime is the 12th, stime the 13th.
    let rest = &stat[stat.rfind(')')? + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks: f64 = f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?;
    // USER_HZ, 100 on every Linux the box runs.
    Some(ticks / 100.0)
}

/// Pins the calling thread, and the threads it starts after, to `list` (Linux).
#[cfg(target_os = "linux")]
fn pin_self(list: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut set = nix::sched::CpuSet::new();
    for part in list.split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                for cpu in a.trim().parse::<usize>()?..=b.trim().parse::<usize>()? {
                    set.set(cpu)?;
                }
            }
            None => set.set(part.trim().parse()?)?,
        }
    }
    nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &set)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_self(_list: &str) -> Result<(), Box<dyn std::error::Error>> {
    Err("--client-cpus needs Linux".into())
}

/// The server's base URL: `--server`, or the one this process starts.
fn base_url(args: &Args) -> String {
    match &args.server {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None => format!("http://127.0.0.1:{}", args.port),
    }
}

/// How long one row's write may be refused (`Overloaded`) or left unknown before the load
/// stops (perf2's F1, feed1248's F2 on #1712).
const LOAD_ROW_DEADLINE: Duration = Duration::from_secs(30);

/// One row of `--load`: sent again while it's `Overloaded`, or its outcome is unknown
/// (`OutcomeUnknown`: a cluster forward cut after the send; a put of the same row is
/// idempotent), for at most [`LOAD_ROW_DEADLINE`]. Anything else, `PartitionUnavailable`
/// from a down node included, stops the load at once with the server's message, which
/// names the partition.
async fn load_row(
    c: &mut DataClient<tonic::transport::Channel>,
    row: u64,
    rows: u64,
    value: &[u8],
    durability: proto::Durability,
) -> Result<(), String> {
    let deadline = Instant::now() + LOAD_ROW_DEADLINE;
    loop {
        let last = match grpc_call(c, Op::Mutate, row, rows, value, durability).await {
            Ok(Outcome::Served) => return Ok(()),
            Ok(Outcome::Refused) => "Overloaded".to_owned(),
            Ok(Outcome::Cross) => unreachable!("a one-row write is in one partition"),
            Err(s) if api_code(&s) == "OutcomeUnknown" => {
                format!("OutcomeUnknown: {}", s.message())
            }
            Err(s) => {
                return Err(format!(
                    "row {}: {} ({:?})",
                    String::from_utf8_lossy(&key(row)),
                    s.message(),
                    s.code()
                ));
            }
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "row {} still refused after {} s: {last}",
                String::from_utf8_lossy(&key(row)),
                LOAD_ROW_DEADLINE.as_secs()
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A step coprime with `rows`, so `k * step % rows` visits every row once, scattered.
fn scatter_step(rows: u64) -> u64 {
    fn gcd(a: u64, b: u64) -> u64 {
        if b == 0 { a } else { gcd(b, a % b) }
    }
    // Primes near 2^32 / φ; the first not dividing `rows` is coprime with it.
    [2_654_435_761u64, 2_654_435_789, 2_654_435_801]
        .into_iter()
        .find(|&p| gcd(p, rows.max(1)) == 1)
        .unwrap_or(1)
}

/// `--load`: the rows through the running server, `Mutate`s from `--clients` callers, so a
/// cluster places each by its split keys. The table is created first where the server
/// serves `Schema.CreateTable` (a single node); elsewhere it must exist already.
async fn load(
    args: &Args,
    channels: &[tonic::transport::Channel],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut schema = proto::schema_client::SchemaClient::new(channels[0].clone());
    let created = schema
        .create_table(proto::CreateTableRequest {
            name: "t".into(),
            families: vec![proto::FamilySpec {
                name: "f".into(),
                ..Default::default()
            }],
            if_missing: true,
        })
        .await;
    match created {
        Ok(_) => {}
        // A cluster serves no schema calls yet (D317's ticket 5): `Unimplemented`. Anything
        // else, `Overloaded` included, stops the load (feed1248's N2 on #1712).
        Err(s) if s.code() == tonic::Code::Unimplemented => {
            println!(
                "load: the server doesn't create tables ({}): using table t as it is",
                s.message()
            );
        }
        Err(s) => return Err(format!("create table t: {s}").into()),
    }
    let started = Instant::now();
    let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let value = Arc::new(vec![b'v'; args.value_bytes]);
    let durability = args.load_durability.proto();
    let step = scatter_step(args.rows);
    // Set by the first caller that fails, so the others stop at their next row rather than
    // loading on to the end (perf2's nit on #1712).
    let failed = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for i in 0..args.clients.max(1) {
        let mut c = DataClient::new(channels[i % channels.len()].clone());
        let (next, value, rows) = (Arc::clone(&next), Arc::clone(&value), args.rows);
        let failed = Arc::clone(&failed);
        tasks.push(tokio::spawn(async move {
            loop {
                if failed.load(Ordering::Relaxed) {
                    return Ok::<(), String>(());
                }
                let k = next.fetch_add(1, Ordering::Relaxed);
                if k >= rows {
                    return Ok(());
                }
                // Rows in a scattered order, every one once, so a cluster's partitions load
                // together rather than one key range at a time (feed1248's N1 on #1712).
                let row = u64::try_from(u128::from(k) * u128::from(step) % u128::from(rows))
                    .expect("below rows");
                if let Err(e) = load_row(&mut c, row, rows, &value, durability).await {
                    failed.store(true, Ordering::Relaxed);
                    return Err(e);
                }
                if (k + 1) % 1_000_000 == 0 {
                    println!(
                        "load: {} rows, {:.0} s",
                        k + 1,
                        started.elapsed().as_secs_f64()
                    );
                }
            }
        }));
    }
    // Every caller ends (the failed flag stops the rest early); the first error is the one
    // reported.
    let mut first = None;
    for t in tasks {
        if let Err(e) = t.await? {
            first.get_or_insert(e);
        }
    }
    if let Some(e) = first {
        return Err(format!("load: {e}").into());
    }
    println!(
        "load: {} rows in {:.0} s",
        args.rows,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Against a running server, nothing is built or started here.
    let (server, shards) = if args.server.is_some() {
        (None, None)
    } else {
        let shards = shards(&args);
        let file = build(&args, shards)?;
        // The server first, so it doesn't inherit the client's CPUs.
        (Some(Server::start(&args, &file, shards)?), Some(shards))
    };
    if let Some(list) = &args.client_cpus {
        pin_self(list)?;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_all(args, server, shards))
}

async fn run_all(
    args: Args,
    server: Option<Server>,
    shards: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = base_url(&args);
    let mut channels = Vec::new();
    for _ in 0..args.conns.max(1) {
        channels.push(connect(&url).await?);
    }
    if args.load {
        load(&args, &channels).await?;
    }
    let zipf = Arc::new(Zipf::new(args.rows));
    let mixes: Vec<Mix> = if args.mix.is_empty() {
        MIXES.to_vec()
    } else {
        args.mix.clone()
    };
    println!(
        "serve_load: {}, {} rows of {} B, {} callers ({} gRPC connections), {} s per row after {} s warm-up, durability {:?}; serve args: {:?}",
        match (&args.server, shards) {
            (Some(url), _) => format!("server {url}"),
            (None, Some(n)) => format!("{n} shards"),
            (None, None) => "a server".to_owned(),
        },
        args.rows,
        args.value_bytes,
        args.clients,
        args.conns.max(1),
        args.secs,
        args.warmup_secs,
        args.durability,
        args.serve_args
    );
    println!(
        "{:<5} {:<8} {:>9} {:>11} {:>8}   per op: p50 / p99 / p99.9 ms",
        "proto", "mix", "ops/s", "CPU us/op", "refused"
    );
    // CPU per operation: the server this process started, or `--server-pid`'s.
    let pid = server.as_ref().map(|s| s.0.id()).or(args.server_pid);
    for &transport in &args.transport {
        for (n, &mix) in mixes.iter().enumerate() {
            let seed = 0x5EED ^ ((n as u64) << 8) ^ (transport as u64);
            if args.warmup_secs > 0 {
                run(
                    &args,
                    &channels,
                    transport,
                    mix,
                    args.warmup_secs,
                    !seed,
                    &zipf,
                )
                .await?;
            }
            let cpu0 = pid.and_then(cpu_seconds);
            let tally = run(&args, &channels, transport, mix, args.secs, seed, &zipf).await?;
            let cpu = cpu0.zip(pid.and_then(cpu_seconds)).map(|(a, b)| b - a);
            let ops = tally.ops();
            let mut line = format!(
                "{:<5} {:<8} {:>9.0} {:>11} {:>8}  ",
                match transport {
                    Transport::Grpc => "grpc",
                    Transport::Http => "http",
                },
                mix.name(),
                ops as f64 / args.secs as f64,
                cpu.filter(|_| ops > 0)
                    .map_or("-".into(), |c| format!("{:.1}", c * 1e6 / ops as f64)),
                tally.refused,
            );
            if tally.cross > 0 {
                line.push_str(&format!(" cross-partition {}", tally.cross));
            }
            for op in OPS {
                let mut lat = tally.lat[op as usize].clone();
                if lat.is_empty() {
                    continue;
                }
                lat.sort_unstable();
                let ms = |q: f64| {
                    lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)].as_secs_f64() * 1000.0
                };
                line.push_str(&format!(
                    " {} {:.3} / {:.3} / {:.3}",
                    op.name(),
                    ms(0.5),
                    ms(0.99),
                    ms(0.999)
                ));
            }
            println!("{line}");
        }
    }
    drop(server);
    Ok(())
}
