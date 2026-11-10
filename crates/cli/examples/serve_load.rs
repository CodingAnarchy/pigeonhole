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
//! Writes use the server's durability default (`GroupSync`).
//!
//! ```text
//! cargo build --release -p pigeonhole-cli --features serve --bin phdb --example serve_load
//! target/release/examples/serve_load --phdb target/release/phdb --dir /tmp/serveload
//! # R1's acceptance: the same, with the server's threads unmasked
//! target/release/examples/serve_load --phdb target/release/phdb --dir /tmp/serveload \
//!     --serve-arg=--affinity --serve-arg=off
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
    #[arg(long)]
    phdb: PathBuf,
    /// A directory for the database (made if missing; the database is built once and reused).
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value_t = 7462)]
    port: u16,
    /// The database's shard count when it's built (default: `phdb serve`'s, half the
    /// physical cores).
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

fn build(args: &Args) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&args.dir)?;
    let file = args.dir.join("serveload.phdb");
    if file.exists() {
        return Ok(file);
    }
    let mut opts = Options::default().create_if_missing(true);
    if let Some(n) = args.shards {
        opts = opts.shards(n);
    }
    let db = Pigeonhole::open(&file, opts)?;
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
    fn start(args: &Args, file: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut cmd = match &args.server_cpus {
            Some(list) => {
                let mut c = Command::new("taskset");
                c.arg("-c").arg(list).arg(&args.phdb);
                c
            }
            None => Command::new(&args.phdb),
        };
        let child = cmd
            .arg("serve")
            .arg(file)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", args.port))
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
}

impl Tally {
    fn add(&mut self, other: Tally) {
        for (a, b) in self.lat.iter_mut().zip(other.lat) {
            a.extend(b);
        }
        self.refused += other.refused;
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

/// One gRPC call: `Ok(true)` served, `Ok(false)` refused (`Overloaded`).
async fn grpc_call(
    c: &mut DataClient<tonic::transport::Channel>,
    op: Op,
    row: u64,
    rows: u64,
    value: &[u8],
) -> Result<bool, tonic::Status> {
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
                ..Default::default()
            })
            .await
            .map(|_| ()),
        Op::Batch => c
            .batch(proto::BatchRequest {
                rows: (0..BATCH_ROWS)
                    .map(|i| row_writes((row + i) % rows, value))
                    .collect(),
                ..Default::default()
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
        Ok(()) => Ok(true),
        Err(s) if s.code() == tonic::Code::Unavailable => Ok(false),
        Err(s) => Err(s),
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
fn http_request(op: Op, row: u64, rows: u64, value: &str) -> (&'static str, String) {
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
        Op::Mutate => ("Mutate", format!(r#"{{"writes":{}}}"#, put(row))),
        Op::Batch => (
            "Batch",
            format!(
                r#"{{"rows":[{}]}}"#,
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
fn http_caller(url: String, mut caller: Caller, rows: u64, stop: Arc<AtomicBool>) -> Tally {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let value = b64(&caller.value);
    let mut tally = Tally::default();
    while !stop.load(Ordering::Relaxed) {
        let (op, row) = caller.next();
        let (rpc, body) = http_request(op, row, rows, &value);
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
        match status {
            200 => tally.lat[op as usize].push(t.elapsed()),
            503 => tally.refused += 1,
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
                tasks.push(tokio::spawn(async move {
                    let mut tally = Tally::default();
                    while !stop.load(Ordering::Relaxed) {
                        let (op, row) = caller.next();
                        let t = Instant::now();
                        match grpc_call(&mut c, op, row, rows, &caller.value).await {
                            Ok(true) => tally.lat[op as usize].push(t.elapsed()),
                            Ok(false) => tally.refused += 1,
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
            let url = format!("http://127.0.0.1:{}", args.port);
            let rows = args.rows;
            let threads: Vec<_> = callers
                .map(|caller| {
                    let (url, stop) = (url.clone(), Arc::clone(&stop));
                    std::thread::spawn(move || http_caller(url, caller, rows, stop))
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let file = build(&args)?;
    // The server first, so it doesn't inherit the client's CPUs.
    let server = Server::start(&args, &file)?;
    if let Some(list) = &args.client_cpus {
        pin_self(list)?;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_all(args, server))
}

async fn run_all(args: Args, server: Server) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{}", args.port);
    let mut channels = Vec::new();
    for _ in 0..args.conns.max(1) {
        channels.push(connect(&url).await?);
    }
    let zipf = Arc::new(Zipf::new(args.rows));
    let mixes: Vec<Mix> = if args.mix.is_empty() {
        MIXES.to_vec()
    } else {
        args.mix.clone()
    };
    println!(
        "serve_load: {} rows of {} B, {} callers ({} gRPC connections), {} s per row after {} s warm-up; serve args: {:?}",
        args.rows,
        args.value_bytes,
        args.clients,
        args.conns.max(1),
        args.secs,
        args.warmup_secs,
        args.serve_args
    );
    println!(
        "{:<5} {:<8} {:>9} {:>11} {:>8}   per op: p50 / p99 / p99.9 ms",
        "proto", "mix", "ops/s", "CPU us/op", "refused"
    );
    let pid = server.0.id();
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
            let cpu0 = cpu_seconds(pid);
            let tally = run(&args, &channels, transport, mix, args.secs, seed, &zipf).await?;
            let cpu = cpu0.zip(cpu_seconds(pid)).map(|(a, b)| b - a);
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
