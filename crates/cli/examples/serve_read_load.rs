//! Unary reads under load against `phdb serve` (#1567): what charging read replies to
//! `--inflight-bytes` costs, and how a burst of large replies is bounded.
//!
//! It creates a database with a table `t` holding small rows (`s0000`..) and large ones
//! (`big0`..`big7`, 1 MiB each), starts `phdb serve` on it as its own process, and runs each
//! scenario for `--secs` with `--clients` concurrent callers over `--conns` gRPC
//! connections. Per scenario it prints the calls per second, latency p50 and p99, and how
//! many were refused (`Overloaded`).
//!
//! The scenarios:
//! - `multiget-small`: `MultiGet` of 100 small rows with `page_bytes` 8 MiB (cli692's shape:
//!   a large reservation trimmed to a small reply);
//! - `multiget-big`: `MultiGet` of the eight 1 MiB rows with `page_bytes` 8 MiB;
//! - `readrow-small`: `ReadRow` of one small row.
//!
//! ```text
//! cargo build --release -p pigeonhole-cli --features serve --bin phdb --example serve_read_load
//! target/release/examples/serve_read_load --phdb target/release/phdb --dir /tmp/readload
//! # a small budget, to see the bound: --serve-arg=--inflight-bytes --serve-arg=67108864
//! ```

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use pigeonhole::{Family, Options, Pigeonhole};

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
mod proto {
    include!("../src/serve/proto/pigeonhole.v1.rs");
}

use proto::data_client::DataClient;

#[derive(Parser, Debug)]
struct Args {
    /// The `phdb` binary to serve with (built with `--features serve`).
    #[arg(long)]
    phdb: PathBuf,
    /// A directory for the database (made if missing).
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value_t = 7461)]
    port: u16,
    /// Seconds per scenario.
    #[arg(long, default_value_t = 5)]
    secs: u64,
    /// Concurrent callers.
    #[arg(long, default_value_t = 64)]
    clients: usize,
    /// gRPC connections they share.
    #[arg(long, default_value_t = 8)]
    conns: usize,
    /// Scenarios to run (all by default).
    #[arg(long)]
    only: Vec<String>,
    /// Extra `phdb serve` arguments, one per flag.
    #[arg(long = "serve-arg", allow_hyphen_values = true)]
    serve_args: Vec<String>,
}

const SMALL: usize = 100;

fn build(dir: &std::path::Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dir)?;
    let file = dir.join("readload.phdb");
    if file.exists() {
        return Ok(file);
    }
    let db = Pigeonhole::open(&file, Options::default().create_if_missing(true))?;
    let t = db.table("t")?.family("f", Family::default()).create()?;
    for i in 0..SMALL {
        t.mutate(format!("s{i:04}").as_bytes())
            .put("f", b"q", &[b'v'; 100])
            .commit()?;
    }
    for i in 0..8 {
        t.mutate(format!("big{i}").as_bytes())
            .put("f", b"q", &vec![b'x'; 1 << 20])
            .commit()?;
    }
    db.flush()?;
    db.close()?;
    Ok(file)
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
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
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    MultiGetSmall,
    MultiGetBig,
    ReadRowSmall,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::MultiGetSmall => "multiget-small",
            Shape::MultiGetBig => "multiget-big",
            Shape::ReadRowSmall => "readrow-small",
        }
    }
}

/// One call: `Ok(true)` served, `Ok(false)` refused (`Overloaded`).
async fn call(c: &mut DataClient<tonic::transport::Channel>, shape: Shape, n: u64) -> bool {
    let r = match shape {
        Shape::MultiGetSmall => c
            .multi_get(proto::MultiGetRequest {
                table: "t".into(),
                keys: (0..SMALL)
                    .map(|i| format!("s{i:04}").into_bytes().into())
                    .collect(),
                page_bytes: 8 << 20,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        Shape::MultiGetBig => c
            .multi_get(proto::MultiGetRequest {
                table: "t".into(),
                keys: (0..8)
                    .map(|i| format!("big{i}").into_bytes().into())
                    .collect(),
                page_bytes: 8 << 20,
                ..Default::default()
            })
            .await
            .map(|_| ()),
        Shape::ReadRowSmall => c
            .read_row(proto::ReadRowRequest {
                table: "t".into(),
                row: format!("s{:04}", n % SMALL as u64).into_bytes().into(),
                ..Default::default()
            })
            .await
            .map(|_| ()),
    };
    match r {
        Ok(()) => true,
        Err(s) if s.code() == tonic::Code::Unavailable => false,
        Err(s) => panic!("{}: {s}", shape.name()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let file = build(&args.dir)?;
    let _server = Server(
        Command::new(&args.phdb)
            .arg("serve")
            .arg(&file)
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", args.port))
            .args(&args.serve_args)
            .spawn()?,
    );
    let url = format!("http://127.0.0.1:{}", args.port);
    let mut channels = Vec::new();
    for _ in 0..args.conns.max(1) {
        channels.push(connect(&url).await?);
    }
    println!("scenario         calls/s    p50 ms    p99 ms   refused");
    for shape in [
        Shape::MultiGetSmall,
        Shape::MultiGetBig,
        Shape::ReadRowSmall,
    ] {
        if !args.only.is_empty() && !args.only.iter().any(|o| o == shape.name()) {
            continue;
        }
        let end = Instant::now() + Duration::from_secs(args.secs);
        let refused = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        for i in 0..args.clients {
            let mut c = DataClient::new(channels[i % channels.len()].clone())
                .max_decoding_message_size(usize::MAX);
            let refused = Arc::clone(&refused);
            tasks.push(tokio::spawn(async move {
                let mut lat = Vec::new();
                let mut n = i as u64;
                while Instant::now() < end {
                    let t = Instant::now();
                    if call(&mut c, shape, n).await {
                        lat.push(t.elapsed());
                    } else {
                        refused.fetch_add(1, Ordering::Relaxed);
                    }
                    n += 1;
                }
                lat
            }));
        }
        let mut lat = Vec::new();
        for t in tasks {
            lat.extend(t.await?);
        }
        lat.sort();
        let ms = |q: f64| {
            lat.get(((lat.len() as f64 * q) as usize).min(lat.len().saturating_sub(1)))
                .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
        };
        println!(
            "{:<15} {:>8.0} {:>9.3} {:>9.3} {:>9}",
            shape.name(),
            lat.len() as f64 / args.secs as f64,
            ms(0.5),
            ms(0.99),
            refused.load(Ordering::Relaxed)
        );
    }
    Ok(())
}
