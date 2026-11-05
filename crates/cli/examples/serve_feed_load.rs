//! D313 ticket 5's acceptance run (R2): what idle feed subscribers cost `phdb serve`'s writes.
//!
//! It creates a database with the change feed on and two tables (`busy`, written, and
//! `tiny`, never written), starts `phdb serve` on it as its own process, and for each
//! scenario opens that many idle `Feed.Subscribe` streams over gRPC (64 to a connection),
//! then runs a put loop (`Data.Mutate` on `busy`) for a fixed time. Per scenario it prints
//! the commit throughput, put latency p50/p99/p99.9, the server process's CPU per put (Linux,
//! from `/proc`), and the events the subscribers received. With `--durability 2` (Buffered,
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
//! ```

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
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
}

/// Feed streams per connection: the server allows 128 HTTP/2 streams on one connection
/// (`listener::MAX_STREAMS`), and a subscribe past that waits for a stream forever.
const STREAMS_PER_CONNECTION: usize = 64;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let file = prepare(&args)?;
    let mut server = Server::start(&args, &file)?;
    let url = format!("http://127.0.0.1:{}", args.port);
    let channel = connect(&url).await?;
    println!(
        "{:<22} {:>10} {:>9} {:>9} {:>9} {:>12} {:>12}",
        "scenario", "puts/s", "p50 us", "p99 us", "p999 us", "cpu us/put", "events/sub"
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
        println!(
            "{name:<22} {:>10.0} {:>9} {:>9} {:>9} {:>12} {:>12.1}",
            r.puts_per_sec,
            r.p50,
            r.p99,
            r.p999,
            r.cpu_us_per_put
                .map_or_else(|| "n/a".into(), |c| format!("{c:.1}")),
            r.events_per_sub
        );
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
        let child = Command::new(&args.phdb)
            .arg("serve")
            .arg(file)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", args.port))
            .arg("--max-streams")
            .arg("1024")
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
            Err(e) => return Err(e.into()),
        }
    }
}

struct Outcome {
    puts_per_sec: f64,
    p50: u64,
    p99: u64,
    p999: u64,
    cpu_us_per_put: Option<f64>,
    events_per_sub: f64,
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
    let mut subs = Vec::with_capacity(subscribers);
    let url = format!("http://127.0.0.1:{}", args.port);
    let mut connections = Vec::new();
    for (i, set) in tables.iter().enumerate() {
        // A connection of its own for each `STREAMS_PER_CONNECTION` subscribers: the
        // writers keep `channel`.
        if i % STREAMS_PER_CONNECTION == 0 {
            connections.push(connect(&url).await?);
        }
        let mut client = FeedClient::new(connections[i / STREAMS_PER_CONNECTION].clone());
        let mut stream = client
            .subscribe(proto::SubscribeRequest {
                tables: set.clone(),
                visible: args.visible,
                ..Default::default()
            })
            .await?
            .into_inner();
        let events = Arc::clone(&events);
        subs.push(tokio::spawn(async move {
            while let Ok(Some(_)) = stream.message().await {
                events.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // Let the streams settle before measuring.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let cpu_before = cpu_seconds(server);
    let started = Instant::now();
    let end = started + Duration::from_secs(args.seconds);
    let mut writers = Vec::with_capacity(args.writers);
    for w in 0..args.writers {
        let mut client = DataClient::new(channel.clone());
        let value = vec![b'v'; args.value_bytes];
        let durability = args.durability;
        writers.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut i = 0u64;
            while Instant::now() < end {
                let row = format!("w{w:03}-{i:012}").into_bytes();
                i += 1;
                let t = Instant::now();
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
                                        kind: Some(proto::value::Kind::Bytes(value.clone().into())),
                                    }),
                                })),
                            }],
                        }),
                        durability,
                    })
                    .await;
                if r.is_ok() {
                    lat.push(t.elapsed().as_micros() as u64);
                }
            }
            lat
        }));
    }
    let mut lat = Vec::new();
    for w in writers {
        lat.extend(w.await?);
    }
    let elapsed = started.elapsed().as_secs_f64();
    let cpu = cpu_before.zip(cpu_seconds(server)).map(|(a, b)| b - a);
    // Give the subscribers a moment to take the last commits, then end them.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for s in &subs {
        s.abort();
    }
    lat.sort_unstable();
    let at = |q: f64| {
        lat.get(((lat.len() as f64 * q) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let puts = lat.len() as f64;
    Ok(Outcome {
        puts_per_sec: puts / elapsed,
        p50: at(0.5),
        p99: at(0.99),
        p999: at(0.999),
        cpu_us_per_put: cpu.map(|c| c * 1e6 / puts.max(1.0)),
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
