//! D313 ticket 5's acceptance run (R2): what idle feed subscribers cost `phdb serve`'s writes.
//!
//! It creates a database with the change feed on and two tables (`busy`, written, and
//! `tiny`, never written), starts `phdb serve` on it as its own process, and for each
//! scenario opens that many idle `Feed.Subscribe` streams over gRPC (64 to a connection),
//! then runs a put loop (`Data.Mutate` on `busy`) for a fixed time. Per scenario it prints
//! the commit throughput, put latency p50/p99/p99.9, the server process's CPU per put (Linux,
//! from `/proc`), the events the subscribers received, and their feed lag: each put's value
//! starts with its send time, and each subscriber measures send-to-arrival for every commit
//! it gets (perf287's D3 on #1397). With `--durability 2` (Buffered,
//! so fsync doesn't set the pace), add `--visible`: a durable feed sees only synced commits.
//!
//! The scenarios:
//! - 0, 1, 16 and 256 subscribers on `busy` (D310's bar: N subscribers cost no more than one);
//! - one subscriber on `tiny` while `busy` takes the writes (perf287's row: a hub on a table
//!   nobody writes still reads past the others' records);
//! - 16 hubs × 16 subscribers: sixteen table sets `{busy, hNN}`, each its own engine
//!   subscription, sixteen streams on each (perf287's row on #1518: per-hub costs add up).
//!
//! ```text
//! cargo build --release -p pigeonhole-cli --features serve --bin phdb --example serve_feed_load
//! target/release/examples/serve_feed_load --phdb target/release/phdb --dir /mnt/nvme/feedload
//! # with server flags, e.g. the feed runtime's size (ticket 5's (A)):
//! target/release/examples/serve_feed_load ... --serve-arg=--feed-threads --serve-arg=4
//! # the load generator and the server on disjoint CPUs (Linux), so the ratio is the server's:
//! target/release/examples/serve_feed_load ... --client-cpus 0-11 --server-cpus 12-47
//! ```
//!
//! Only one subscriber in `--lag-every` (16) decodes its events for the lag columns; the
//! others count frames through a codec that decodes nothing, so 256 subscribers cost the
//! client little (perf287 on #1589). `--h2-conn-window`, `--h2-stream-window` and
//! `--h2-adaptive` (on by default) set the client's HTTP/2 receive windows, which pace the
//! server's sends.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use pigeonhole::{Family, FeedRetention, Options, Pigeonhole};

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
mod proto {
    include!("../src/serve/proto/pigeonhole.v1.rs");
}

use proto::data_client::DataClient;
use proto::feed_client::FeedClient;

#[derive(Parser, Debug)]
struct Args {
    /// The `phdb` binary to serve with (built with `--features serve`).
    #[arg(long, default_value = "target/release/phdb")]
    phdb: PathBuf,
    /// A directory for the database (created; its contents are replaced).
    #[arg(long)]
    dir: PathBuf,
    /// The port `phdb serve` listens on, on loopback.
    #[arg(long, default_value_t = 7458)]
    port: u16,
    /// Shards for the database.
    #[arg(long, default_value_t = 4)]
    shards: usize,
    /// Concurrent writers in the put loop.
    #[arg(long, default_value_t = 16)]
    writers: usize,
    /// Each scenario's put loop, in seconds.
    #[arg(long, default_value_t = 30)]
    seconds: u64,
    /// Bytes per value written.
    #[arg(long, default_value_t = 100)]
    value_bytes: usize,
    /// The subscriber counts on `busy` to run.
    #[arg(long, value_delimiter = ',', default_value = "0,1,16,256")]
    subscribers: Vec<usize>,
    /// Durability of the puts: 0 the writer default, else the `Durability` enum's number.
    #[arg(long, default_value_t = 0)]
    durability: i32,
    /// Subscribe to visible feeds (commits as they become visible) rather than durable ones.
    /// Needed with `--durability 2` (Buffered): a durable feed delivers only synced commits.
    #[arg(long)]
    visible: bool,
    /// An argument passed through to `phdb serve`, repeatable (for example
    /// `--serve-arg=--feed-threads --serve-arg=4`).
    #[arg(long = "serve-arg", allow_hyphen_values = true)]
    serve_args: Vec<String>,
    /// One subscriber in this many decodes its events and measures lag; the others only
    /// count frames, so the load generator costs little (perf287 on #1589).
    #[arg(long, default_value_t = 16)]
    lag_every: usize,
    /// Run this process (the writers and subscribers) on these CPUs, e.g. `0-11` (Linux).
    #[arg(long, value_name = "LIST")]
    client_cpus: Option<String>,
    /// Run `phdb serve` on these CPUs, through `taskset -c` (Linux).
    #[arg(long, value_name = "LIST")]
    server_cpus: Option<String>,
    /// The client's HTTP/2 connection receive window, in bytes (default: hyper's, 5 MiB).
    #[arg(long, value_name = "BYTES")]
    h2_conn_window: Option<u32>,
    /// The client's HTTP/2 per-stream receive window, in bytes (default: hyper's, 2 MiB).
    #[arg(long, value_name = "BYTES")]
    h2_stream_window: Option<u32>,
    /// Offer puts at this total rate, puts/s across all writers, on a fixed schedule of
    /// absolute due times kept by a pacer thread (it sleeps, then spins the last 200 us);
    /// unset, the writers run flat out. With a rate, a put's latency counts from when it was
    /// due, so a server that falls behind can't hide it (perf287's offered-load acceptance).
    /// The pacer spins one of the client's cores (`--client-cpus` should leave room for it).
    /// Each due put goes to the writer with the fewest outstanding; when every writer is busy it
    /// waits, still timed from its due time. Each row's `pacer:` line says how late puts left
    /// the pacer and how many waited for a writer.
    #[arg(long, value_name = "PUTS_PER_SEC")]
    put_rate: Option<f64>,
    /// Let the client's HTTP/2 windows follow the measured bandwidth-delay product (on by
    /// default: fixed windows let about 18 ms of events queue per stream, perf287 on #1397).
    /// `--h2-adaptive false` uses the fixed windows.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    h2_adaptive: bool,
}

/// Feed streams per connection: the server allows 128 HTTP/2 streams on one connection
/// (`listener::MAX_STREAMS`), and a subscribe past that waits for a stream forever.
const STREAMS_PER_CONNECTION: usize = 64;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    epoch();
    let file = prepare(&args)?;
    // The server first, so it doesn't inherit the client's CPUs.
    let server = Server::start(&args, &file)?;
    // Then this process's CPUs, before the runtime starts its threads: they inherit them.
    if let Some(list) = &args.client_cpus {
        pin_self(list)?;
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_all(args, server))
}

/// Pins the calling thread, and the threads it starts after, to `list` (Linux).
#[cfg(target_os = "linux")]
fn pin_self(list: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut set = nix::sched::CpuSet::new();
    for cpu in cpu_list(list)? {
        set.set(cpu)?;
    }
    nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &set)?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_self(_list: &str) -> Result<(), Box<dyn std::error::Error>> {
    Err("--client-cpus needs Linux".into())
}

/// `0-3,8,10-11` as CPU numbers.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cpu_list(list: &str) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for part in list.split(',').filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => out.extend(a.trim().parse::<usize>()?..=b.trim().parse::<usize>()?),
            None => out.push(part.trim().parse()?),
        }
    }
    Ok(out)
}

async fn run_all(args: Args, mut server: Server) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{}", args.port);
    let channel = connect(&url, &args).await?;
    println!(
        "client: lag from 1 subscriber in {}; h2 windows: connection {}, stream {}{}; CPUs: client {}, server {}",
        args.lag_every.max(1),
        args.h2_conn_window
            .map_or("default".into(), |w| w.to_string()),
        args.h2_stream_window
            .map_or("default".into(), |w| w.to_string()),
        if args.h2_adaptive { ", adaptive" } else { "" },
        args.client_cpus.as_deref().unwrap_or("any"),
        args.server_cpus.as_deref().unwrap_or("any"),
    );
    if !args.serve_args.is_empty() {
        println!("phdb serve {}", args.serve_args.join(" "));
    }
    if let Some(rate) = args.put_rate {
        println!("offered: {rate:.0} puts/s across {} writers", args.writers);
    }
    println!(
        "{:<22} {:>10} {:>9} {:>9} {:>9} {:>12} {:>12} {:>11} {:>11} {:>7} {:>8} {:>8}",
        "scenario",
        "puts/s",
        "p50 us",
        "p99 us",
        "p999 us",
        "cpu us/put",
        "events/sub",
        "lag p50 us",
        "lag p99 us",
        "behind",
        "rejoined",
        "dropped"
    );
    let busy = |n: usize| vec![vec!["busy".to_owned()]; n];
    let mut scenarios: Vec<(String, Vec<Vec<String>>)> = args
        .subscribers
        .iter()
        .map(|&n| (format!("{n} on busy"), busy(n)))
        .collect();
    scenarios.push(("1 on tiny".into(), vec![vec!["tiny".to_owned()]]));
    scenarios.push((
        "16 hubs x 16".into(),
        (0..16)
            .flat_map(|h| std::iter::repeat_n(vec!["busy".to_owned(), format!("h{h:02}")], 16))
            .collect(),
    ));
    for (name, subs) in scenarios {
        let r = run(&args, &channel, server.pid(), &subs).await?;
        let lag = |q: f64| {
            r.lag
                .quantile(q)
                .map_or_else(|| "-".into(), |v| v.to_string())
        };
        println!(
            "{name:<22} {:>10.0} {:>9} {:>9} {:>9} {:>12} {:>12.1} {:>11} {:>11} {:>7} {:>8} {:>8}",
            r.puts_per_sec,
            r.p50,
            r.p99,
            r.p999,
            r.cpu_us_per_put
                .map_or_else(|| "n/a".into(), |c| format!("{c:.1}")),
            r.events_per_sub,
            lag(0.5),
            lag(0.99),
            r.fell_behind,
            r.rejoined,
            r.dropped
        );
        if let Some(p) = &r.pacer {
            println!(
                "  pacer: {} puts sent ({} waited for a busy writer; at most {} outstanding), {} skipped (pacer late); left late us p50 {} p99 {} max {}; service us (from send) p50 {} p99 {} p999 {} max {}",
                p.sent,
                p.queued,
                p.max_outstanding,
                p.late_skipped,
                p.late(0.5),
                p.late(0.99),
                p.late(1.0),
                p.service(0.5),
                p.service(0.99),
                p.service(0.999),
                p.service(1.0)
            );
            // perf287 on the pacer: more than 0.1% skipped means the client couldn't keep the
            // schedule, so the row offered less than the rate.
            if p.late_skipped * 1000 > p.sent {
                println!(
                    "  WARNING: the pacer skipped {} of {} puts (over 0.1%): the client couldn't keep the schedule; this row offered less than --put-rate",
                    p.late_skipped,
                    p.sent + p.late_skipped
                );
            }
        }
    }
    server.stop();
    Ok(())
}

/// The database: the change feed on, `busy` and `tiny`, closed again for the server.
fn prepare(args: &Args) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let _ = std::fs::remove_dir_all(&args.dir);
    std::fs::create_dir_all(&args.dir)?;
    let file = args.dir.join("load.phdb");
    let db = Pigeonhole::open(
        &file,
        Options::default()
            .create_if_missing(true)
            .shards(args.shards)
            .change_feed(FeedRetention::bytes(1 << 30)),
    )?;
    for t in ["busy", "tiny"]
        .into_iter()
        .map(str::to_owned)
        .chain((0..16).map(|i| format!("h{i:02}")))
    {
        db.table(&t)?.family("f", Family::default()).create()?;
    }
    db.close()?;
    Ok(file)
}

struct Server(Child);

impl Server {
    fn start(args: &Args, file: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
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
            .arg("--max-streams")
            .arg("1024")
            .args(&args.serve_args)
            .spawn()?;
        Ok(Self(child))
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }

    fn stop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn connect(
    url: &str,
    args: &Args,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let endpoint = tonic::transport::Channel::from_shared(url.to_owned())?
            .initial_connection_window_size(args.h2_conn_window)
            .initial_stream_window_size(args.h2_stream_window)
            .http2_adaptive_window(args.h2_adaptive);
        match endpoint.connect().await {
            Ok(c) => return Ok(c),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// The server's feed counters, from `Admin.Metrics` (Prometheus text): name to value.
async fn feed_counters(
    channel: &tonic::transport::Channel,
) -> Result<std::collections::HashMap<String, u64>, Box<dyn std::error::Error>> {
    let text = proto::admin_client::AdminClient::new(channel.clone())
        .metrics(proto::Empty {})
        .await?
        .into_inner()
        .text;
    Ok(text
        .lines()
        .filter(|l| l.starts_with("pigeonhole_serve_feed_"))
        .filter_map(|l| {
            let (name, value) = l.rsplit_once(' ')?;
            Some((name.to_owned(), value.parse().ok()?))
        })
        .collect())
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

/// When the run started: put values carry their send time as nanoseconds since this, which
/// the subscribers, in the same process, compare with their arrival.
fn epoch() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

fn now_nanos() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

/// Feed lag, send to arrival, in microseconds: a log-bucketed histogram (about 5% wide), one
/// per subscriber so the subscribers share nothing, merged at the end.
#[derive(Clone)]
struct Lag(Vec<u64>);

impl Lag {
    /// Buckets up to about 100 s.
    const BUCKETS: usize = 400;

    fn new() -> Self {
        Self(vec![0; Self::BUCKETS])
    }

    fn bucket(us: u64) -> usize {
        ((((us + 1) as f64).ln() / 1.05f64.ln()) as usize).min(Self::BUCKETS - 1)
    }

    fn record(&mut self, us: u64) {
        self.0[Self::bucket(us)] += 1;
    }

    fn merge(&mut self, other: &Self) {
        for (a, b) in self.0.iter_mut().zip(&other.0) {
            *a += b;
        }
    }

    /// The `q` quantile's bucket, as its upper bound in microseconds.
    fn quantile(&self, q: f64) -> Option<u64> {
        let total: u64 = self.0.iter().sum();
        if total == 0 {
            return None;
        }
        let want = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, n) in self.0.iter().enumerate() {
            seen += n;
            if seen >= want {
                return Some(1.05f64.powi(i as i32 + 1) as u64);
            }
        }
        None
    }
}

struct Outcome {
    lag: Lag,
    /// Server-side: subscribers that fell behind the shared ring, and that rejoined it.
    fell_behind: u64,
    rejoined: u64,
    /// Client-side: streams the server ended before the scenario did.
    dropped: u64,
    puts_per_sec: f64,
    p50: u64,
    p99: u64,
    p999: u64,
    cpu_us_per_put: Option<f64>,
    events_per_sub: f64,
    /// With `--put-rate`: how the pacer kept its schedule.
    pacer: Option<Paced>,
}

/// How the pacer kept its schedule in one scenario.
struct Paced {
    /// Puts handed to a writer.
    sent: u64,
    /// Of those, puts that found every writer busy and waited behind another (the server was
    /// behind; their latency still counts from their due time), and the most puts handed out
    /// and not yet done at once.
    queued: u64,
    max_outstanding: u64,
    /// Puts not offered: the pacer itself reached them after the next was due (descheduled),
    /// so they're skipped rather than sent late in a burst.
    late_skipped: u64,
    /// How late each handed-out put left the pacer, in microseconds, sorted.
    late_us: Vec<u64>,
    /// Each put's service time, from its send (not its due time), in microseconds, sorted.
    service_us: Vec<u64>,
}

impl Paced {
    fn late(&self, q: f64) -> u64 {
        quantile(&self.late_us, q)
    }

    fn service(&self, q: f64) -> u64 {
        quantile(&self.service_us, q)
    }
}

/// The `q` quantile of sorted microseconds (0 when empty).
fn quantile(sorted: &[u64], q: f64) -> u64 {
    let n = sorted.len();
    sorted
        .get(((n as f64 * q) as usize).min(n.saturating_sub(1)))
        .copied()
        .unwrap_or(0)
}

/// `--put-rate`'s schedule: put `k` is due at `started + k / rate`, an absolute time, so
/// nothing drifts. Each due time goes to the writers in turn. The thread sleeps until
/// [`SPIN`] before a due time and spins the rest, so a put leaves within microseconds of it.
/// It goes to the writer with the fewest outstanding, so a put waits only when every writer is
/// busy: a server that falls behind is measured from the due times (open loop), never by
/// dropping its slow puts. Only the pacer's own lateness skips puts, so a descheduled pacer
/// doesn't send a burst.
fn pace(
    started: Instant,
    end: Instant,
    rate: f64,
    writers: Vec<(tokio::sync::mpsc::UnboundedSender<Instant>, Arc<AtomicU64>)>,
) -> Paced {
    /// How long before a due time the pacer stops sleeping and spins: past the OS's sleep
    /// overshoot (tens of microseconds on Linux).
    const SPIN: Duration = Duration::from_micros(200);
    let due = |k: u64| started + Duration::from_secs_f64(k as f64 / rate.max(1e-9));
    let mut paced = Paced {
        sent: 0,
        queued: 0,
        max_outstanding: 0,
        late_skipped: 0,
        late_us: Vec::new(),
        service_us: Vec::new(),
    };
    let mut k = 0u64;
    loop {
        let at = due(k);
        if at >= end {
            break;
        }
        let now = Instant::now();
        if at > now + SPIN {
            std::thread::sleep(at - now - SPIN);
        }
        while Instant::now() < at {
            std::hint::spin_loop();
        }
        let left = Instant::now();
        // Reached after the next put was due (the thread was descheduled): skip it rather
        // than send a burst.
        if left >= due(k + 1) {
            paced.late_skipped += 1;
        } else {
            // The writer with the fewest outstanding, from a rotating start so ties spread. A
            // writer counts a put from here until the put is done, so an idle one is 0.
            let n = writers.len();
            let (tx, waiting) = (0..n)
                .map(|i| &writers[(k as usize + i) % n])
                .min_by_key(|(_, w)| w.load(Ordering::Relaxed))
                .expect("at least one writer");
            let before = waiting.fetch_add(1, Ordering::Relaxed);
            if tx.send(at).is_ok() {
                paced.sent += 1;
                paced.late_us.push((left - at).as_micros() as u64);
                if before > 0 {
                    paced.queued += 1;
                }
                let total: u64 = writers.iter().map(|(_, w)| w.load(Ordering::Relaxed)).sum();
                paced.max_outstanding = paced.max_outstanding.max(total);
            } else {
                waiting.fetch_sub(1, Ordering::Relaxed);
            }
        }
        k += 1;
    }
    paced.late_us.sort_unstable();
    paced
}

async fn run(
    args: &Args,
    channel: &tonic::transport::Channel,
    server: u32,
    tables: &[Vec<String>],
) -> Result<Outcome, Box<dyn std::error::Error>> {
    // The idle subscribers, one per table set listed: each drains its stream and counts
    // what it gets.
    let subscribers = tables.len();
    let events = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(());
    let mut subs = Vec::with_capacity(subscribers);
    let url = format!("http://127.0.0.1:{}", args.port);
    let mut connections = Vec::new();
    for (i, set) in tables.iter().enumerate() {
        // A connection of its own for each `STREAMS_PER_CONNECTION` subscribers: the
        // writers keep `channel`.
        if i % STREAMS_PER_CONNECTION == 0 {
            connections.push(connect(&url, args).await?);
        }
        let conn = connections[i / STREAMS_PER_CONNECTION].clone();
        let request = proto::SubscribeRequest {
            tables: set.clone(),
            visible: args.visible,
            ..Default::default()
        };
        let events = Arc::clone(&events);
        let dropped = Arc::clone(&dropped);
        let mut stop = stop_rx.clone();
        if i % args.lag_every.max(1) != 0 {
            // A counting subscriber: frames, not decoded.
            let mut stream = count_frames(conn, request).await?;
            subs.push(tokio::spawn(async move {
                loop {
                    let n = tokio::select! {
                        m = stream.message() => m,
                        _ = stop.changed() => break,
                    };
                    let Ok(Some(_)) = n else {
                        dropped.fetch_add(1, Ordering::Relaxed);
                        break;
                    };
                    events.fetch_add(1, Ordering::Relaxed);
                }
                Lag::new()
            }));
            continue;
        }
        let mut stream = FeedClient::new(conn).subscribe(request).await?.into_inner();
        subs.push(tokio::spawn(async move {
            let mut lag = Lag::new();
            loop {
                let e = tokio::select! {
                    m = stream.message() => m,
                    _ = stop.changed() => break,
                };
                let Ok(Some(e)) = e else {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    break;
                };
                events.fetch_add(1, Ordering::Relaxed);
                if let Some(proto::feed_event::Event::Commit(c)) = e.event {
                    let now = now_nanos();
                    for ch in &c.changes {
                        if let Some(proto::Value {
                            kind: Some(proto::value::Kind::Bytes(b)),
                        }) = &ch.value
                            && let Some(sent) = b.get(..8)
                        {
                            let sent = u64::from_le_bytes(sent.try_into().unwrap_or_default());
                            lag.record(now.saturating_sub(sent) / 1000);
                        }
                    }
                }
            }
            lag
        }));
    }
    // Let the streams settle before measuring.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let cpu_before = cpu_seconds(server);
    let metrics_before = feed_counters(channel).await?;
    let started = Instant::now();
    let end = started + Duration::from_secs(args.seconds);
    let mut writers = Vec::with_capacity(args.writers);
    // --put-rate: one thread keeps the schedule (perf287 on #1397: tokio's ~1 ms timer made a
    // paced put's latency, counted from its due time, up to a tick late).
    let (pacer, mut pacer_rx) = match args.put_rate {
        Some(rate) => {
            let (txs, rxs): (Vec<_>, Vec<_>) = (0..args.writers)
                .map(|_| {
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Instant>();
                    let waiting = Arc::new(AtomicU64::new(0));
                    ((tx, Arc::clone(&waiting)), (rx, waiting))
                })
                .unzip();
            let pacer = std::thread::Builder::new()
                .name("pacer".into())
                .spawn(move || pace(started, end, rate, txs))?;
            (Some(pacer), Some(rxs))
        }
        None => (None, None),
    };
    for w in 0..args.writers {
        let mut client = DataClient::new(channel.clone());
        let value_bytes = args.value_bytes.max(8);
        let durability = args.durability;
        // With --put-rate, the pacer thread hands this writer its puts' due times.
        let mut ticks = pacer_rx.as_mut().map(|rxs| rxs.remove(0));
        writers.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            // Each put's own time on the wire, from when it was sent: with --put-rate, apart
            // from any wait for a busy writer.
            let mut service = Vec::new();
            let mut i = 0u64;
            while Instant::now() < end {
                // Paced: the put's due time, and its latency counts from then.
                let due = match &mut ticks {
                    Some((rx, _)) => match rx.recv().await {
                        Some(d) => Some(d),
                        None => break,
                    },
                    None => None,
                };
                let row = format!("w{w:03}-{i:012}").into_bytes();
                i += 1;
                // The send time first: the subscribers' lag is measured from it.
                let mut value = vec![b'v'; value_bytes];
                value[..8].copy_from_slice(&now_nanos().to_le_bytes());
                let t = due.unwrap_or_else(Instant::now);
                let sent = Instant::now();
                let r = client
                    .mutate(proto::MutateRequest {
                        writes: Some(proto::RowWrites {
                            row: Some(proto::RowKey {
                                table: "busy".into(),
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
                        }),
                        durability,
                    })
                    .await;
                if r.is_ok() {
                    lat.push(t.elapsed().as_micros() as u64);
                    service.push(sent.elapsed().as_micros() as u64);
                }
                // Done with this put: the pacer may count this writer idle again.
                if let Some((_, outstanding)) = &ticks {
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                }
            }
            (lat, service)
        }));
    }
    let mut lat = Vec::new();
    let mut service = Vec::new();
    for w in writers {
        let (l, sv) = w.await?;
        lat.extend(l);
        service.extend(sv);
    }
    service.sort_unstable();
    let pacer = match pacer {
        Some(p) => {
            let mut p = p.join().map_err(|_| "the pacer thread panicked")?;
            p.service_us = service;
            Some(p)
        }
        None => None,
    };
    let elapsed = started.elapsed().as_secs_f64();
    let cpu = cpu_before.zip(cpu_seconds(server)).map(|(a, b)| b - a);
    // Give the subscribers a moment to take the last commits, then end them.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = stop_tx.send(());
    let mut lag = Lag::new();
    for s in subs {
        lag.merge(&s.await?);
    }
    let metrics_after = feed_counters(channel).await?;
    let delta = |name: &str| {
        metrics_after
            .get(name)
            .copied()
            .unwrap_or(0)
            .saturating_sub(metrics_before.get(name).copied().unwrap_or(0))
    };
    lat.sort_unstable();
    let at = |q: f64| {
        lat.get(((lat.len() as f64 * q) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let puts = lat.len() as f64;
    Ok(Outcome {
        lag,
        fell_behind: delta("pigeonhole_serve_feed_fell_behind_total"),
        rejoined: delta("pigeonhole_serve_feed_rejoined_total"),
        dropped: dropped.load(Ordering::Relaxed),
        puts_per_sec: puts / elapsed,
        p50: at(0.5),
        p99: at(0.99),
        p999: at(0.999),
        cpu_us_per_put: cpu.map(|c| c * 1e6 / puts.max(1.0)),
        pacer,
        events_per_sub: if subscribers == 0 {
            0.0
        } else {
            events.load(Ordering::Relaxed) as f64 / subscribers as f64
        },
    })
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
