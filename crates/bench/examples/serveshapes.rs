//! `phdb serve`'s measured shapes (#1828), one per run, for `scripts/instructions-per-cell.sh`:
//! gRPC calls through the router a single node serves, in process, with no listener or
//! connection. A call takes the router's middleware (the token check, `prepay`'s message
//! read and reservation, the concurrency limit), tonic's decode, the service's admission and
//! handler, the conversion and the reply's encode: what `phdb serve` adds to the engine's own
//! work, which the engine's shapes count.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example serveshapes -- SHAPE ITERATIONS DIR
//! ```
//!
//! | Shape | One iteration | Units (`units N` on stderr) |
//! |---|---|---|
//! | `serve-get` | 200 `Data.Get`s of one 100-byte cell, each of another row | calls |
//! | `serve-read-row` | 20 `Data.ReadRow`s of a whole 100-cell row (40-byte values) | calls |
//! | `serve-scan` | 2 `Data.Scan`s of 1,000 one-cell rows, each one page | calls |
//! | `serve-mutate` | 200 `Data.Mutate`s of one 100-byte cell to a new row | calls |
//!
//! The router comes from `pigeonhole_cli::serve_bench::router`, built by the construction
//! `phdb serve` serves with (`serve::single`), with a token file (so every call checks its
//! token) and one shard. The data is written in setup through the library, before the
//! server opens the file; one unmeasured pass of the shape's calls warms the caches.
//!
//! **What's counted:** the calls run on a current-thread runtime built for the measurement:
//! its own thread (the handlers, the async reads and the writes' tasks) and its blocking
//! pool's threads (`ReadRow` and `Scan` run there), each counted from its start. The shards'
//! threads, which the server owns, are not: the engine's shapes count that side. How often a
//! waiting thread parks and is polled again can depend on timing; the CI runs show how much.
//!
//! The binary's allocator is the system's, not the jemalloc the `phdb` binary installs
//! (D322): the counts are the code's, under the workflow's fixed glibc tunables.
#[path = "support/measure.rs"]
mod measure;
use measure::Measured;

use std::cell::RefCell;
use std::path::Path;

use axum::body::Body;
use axum::http::{Request, header};
use pigeonhole::{Family, Options, Pigeonhole};
use prost::Message;
use tower::ServiceExt;

#[allow(clippy::all, clippy::pedantic, dead_code, missing_docs)]
mod proto {
    include!("../../cli/src/serve/proto/pigeonhole.v1.rs");
}

const TOKEN: &str = "serveshapes-0123456789abcdef";
/// Rows of one 100-byte cell (`g:`), for `serve-get` and `serve-scan`.
const ROWS: usize = 10_000;
/// Rows of 100 cells (`w:`), for `serve-read-row`.
const WIDE_ROWS: usize = 100;
const TS: u64 = 1_600_000_000_000_000;

fn main() {
    measure::fix_hash_seeds();
    let mut args = std::env::args().skip(1);
    let usage = "usage: serveshapes SHAPE ITERATIONS DIR";
    let shape = args.next().expect(usage);
    let iters: usize = args.next().expect(usage).parse().expect("ITERATIONS");
    let base = std::path::PathBuf::from(args.next().expect(usage));
    let dir = base.join(format!("phdb-serveshapes-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create the store directory");
    let setup_only = std::env::var_os("SHAPE_SETUP_ONLY").is_some();
    let file = dir.join("s.phdb");
    write_data(&file);
    let tokens = dir.join("tokens");
    std::fs::write(&tokens, format!("write {TOKEN}\n")).unwrap();
    let served = pigeonhole_cli::serve_bench::router(
        [
            "phdb".as_ref(),
            "serve".as_ref(),
            file.as_os_str(),
            "--token-file".as_ref(),
            tokens.as_os_str(),
            "--shards".as_ref(),
            "1".as_ref(),
            "--io-threads".as_ref(),
            "1".as_ref(),
            "--blocking-threads".as_ref(),
            "1".as_ref(),
            "--affinity".as_ref(),
            "off".as_ref(),
        ],
        &mut std::io::stderr(),
    )
    .expect("the router");
    let calls = calls(&shape, iters);
    // The warm-up: the shape's reads once each (their blocks and rows cached), or one
    // write per row of another prefix, on a runtime of its own.
    let warm = if shape == "serve-mutate" {
        calls_of_mutate("m0", 200)
    } else {
        calls.clone()
    };
    runtime(false).block_on(run(&served.router, &warm));
    let mut units = 0;
    if !setup_only {
        units = shape_serve(&served.router, &calls);
    }
    eprintln!("units {units}");
    served.close().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

/// Table `t`: `ROWS` rows of one 100-byte cell and `WIDE_ROWS` rows of 100 40-byte cells,
/// at fixed timestamps, flushed.
fn write_data(file: &Path) {
    let db = Pigeonhole::open(
        file,
        Options::default()
            .shards(1)
            .durability(pigeonhole::Durability::Buffered),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let mut ts = TS;
    for r in 0..ROWS {
        ts += 1;
        t.mutate(&row("g", r))
            .put_at("f", b"q", ts, &[7; 100])
            .commit()
            .unwrap();
    }
    for r in 0..WIDE_ROWS {
        let mut m = t.mutate(&row("w", r));
        for q in 0..100 {
            ts += 1;
            m = m.put_at("f", format!("q{q:03}").as_bytes(), ts, &[7; 40]);
        }
        m.commit().unwrap();
    }
    db.flush().unwrap();
    drop(t);
    db.close().unwrap();
}

fn row(prefix: &str, i: usize) -> Vec<u8> {
    format!("{prefix}:{i:08}").into_bytes()
}

/// One gRPC call: its path and its framed message.
#[derive(Clone)]
struct Call {
    path: &'static str,
    framed: Vec<u8>,
}

impl Call {
    fn new(path: &'static str, message: &impl Message) -> Self {
        let body = message.encode_to_vec();
        let mut framed = Vec::with_capacity(5 + body.len());
        framed.push(0);
        framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
        framed.extend_from_slice(&body);
        Call { path, framed }
    }
}

/// The shape's calls, encoded (outside the count).
fn calls(shape: &str, iters: usize) -> Vec<Call> {
    match shape {
        "serve-get" => (0..200 * iters)
            .map(|i| {
                Call::new(
                    "/pigeonhole.v1.Data/Get",
                    &proto::GetRequest {
                        table: "t".into(),
                        row: row("g", i * 7_919 % ROWS).into(),
                        family: "f".into(),
                        qualifier: b"q".to_vec().into(),
                        ..Default::default()
                    },
                )
            })
            .collect(),
        "serve-read-row" => (0..20 * iters)
            .map(|i| {
                Call::new(
                    "/pigeonhole.v1.Data/ReadRow",
                    &proto::ReadRowRequest {
                        table: "t".into(),
                        row: row("w", i * 37 % WIDE_ROWS).into(),
                        ..Default::default()
                    },
                )
            })
            .collect(),
        "serve-scan" => (0..2 * iters)
            .map(|i| {
                // 1,000 rows from a prefix of 1,000 (`g:0000x...`), one page.
                Call::new(
                    "/pigeonhole.v1.Data/Scan",
                    &proto::ScanRequest {
                        table: "t".into(),
                        range: Some(proto::KeyRange {
                            range: Some(proto::key_range::Range::Prefix(
                                format!("g:0000{}", i % 10).into_bytes().into(),
                            )),
                        }),
                        limit: 1_000,
                        page_bytes: 8 << 20,
                        ..Default::default()
                    },
                )
            })
            .collect(),
        "serve-mutate" => calls_of_mutate("m1", 200 * iters),
        other => {
            panic!("unknown shape {other}: serve-get, serve-read-row, serve-scan or serve-mutate")
        }
    }
}

/// `n` Mutates of one 100-byte cell, each to a new row under `prefix`.
fn calls_of_mutate(prefix: &str, n: usize) -> Vec<Call> {
    (0..n)
        .map(|i| {
            Call::new(
                "/pigeonhole.v1.Data/Mutate",
                &proto::MutateRequest {
                    writes: Some(proto::RowWrites {
                        row: Some(proto::RowKey {
                            table: "t".into(),
                            row: row(prefix, i).into(),
                        }),
                        writes: vec![proto::Write {
                            op: Some(proto::write::Op::Put(proto::Put {
                                family: "f".into(),
                                qualifier: b"q".to_vec().into(),
                                timestamp: Some(TS + i as u64),
                                value: Some(proto::Value {
                                    kind: Some(proto::value::Kind::Bytes(vec![7; 100].into())),
                                }),
                            })),
                        }],
                    }),
                    ..Default::default()
                },
            )
        })
        .collect()
}

thread_local! {
    /// A measured runtime's blocking thread counts its whole life.
    static COUNTED: RefCell<Option<Measured>> = const { RefCell::new(None) };
}

/// A current-thread runtime with one blocking thread (as `--blocking-threads 1` bounds the
/// service's). `measured`: each of its blocking threads counts from its start to its end.
fn runtime(measured: bool) -> tokio::runtime::Runtime {
    let mut b = tokio::runtime::Builder::new_current_thread();
    b.enable_all().max_blocking_threads(1);
    if measured {
        b.on_thread_start(start_counting)
            .on_thread_stop(stop_counting);
    }
    b.build().unwrap()
}

#[inline(never)]
fn start_counting() {
    COUNTED.with(|c| *c.borrow_mut() = Some(Measured::start()));
}

#[inline(never)]
fn stop_counting() {
    COUNTED.with(|c| drop(c.borrow_mut().take()));
}

/// The measured calls: on a runtime built for them, its thread counted while it runs them,
/// and its blocking threads from their start until the runtime's shutdown ends them.
#[inline(never)]
fn shape_serve(router: &axum::Router, calls: &[Call]) -> usize {
    std::thread::scope(|s| {
        s.spawn(|| {
            let rt = runtime(true);
            let n = {
                let _measured = Measured::start();
                rt.block_on(run(router, calls))
            };
            // Its blocking threads end here, each stopping its count.
            rt.shutdown_timeout(std::time::Duration::from_secs(60));
            n
        })
        .join()
        .expect("the measured thread panicked")
    })
}

/// Makes `calls` one after another; each must answer `grpc-status` 0.
async fn run(router: &axum::Router, calls: &[Call]) -> usize {
    for call in calls {
        let req = Request::post(call.path)
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("te", "trailers")
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .body(Body::from(call.framed.clone()))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        let mut status = resp
            .headers()
            .get("grpc-status")
            .map(|v| v.as_bytes().to_vec());
        let mut body = resp.into_body();
        let mut bytes = 0;
        while let Some(frame) = std::future::poll_fn(|cx| {
            http_body::Body::poll_frame(std::pin::Pin::new(&mut body), cx)
        })
        .await
        {
            let frame = frame.unwrap();
            match frame.into_data() {
                Ok(data) => bytes += data.len(),
                Err(frame) => {
                    if let Some(t) = frame.trailers_ref()
                        && let Some(v) = t.get("grpc-status")
                    {
                        status = Some(v.as_bytes().to_vec());
                    }
                }
            }
        }
        assert_eq!(
            status.as_deref(),
            Some(&b"0"[..]),
            "{} answered grpc-status {status:?}",
            call.path
        );
        std::hint::black_box(bytes);
    }
    calls.len()
}
