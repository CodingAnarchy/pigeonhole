//! `serving-server`: owns the database and serves the example's API.
//!
//! ```sh
//! cargo run -p pigeonhole-example-serving --bin serving-server -- jobs.phdb 127.0.0.1:7070
//! ```
//!
//! The token, if any, comes from the environment (`SERVING_TOKEN`), never from a flag, so it
//! doesn't show in process listings or shell history. Without one, the server binds only a
//! loopback address: anything else needs TLS and auth in front (see the guide).

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use pigeonhole_example_serving::{App, Config, serve};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (Some(path), Some(addr)) = (args.next(), args.next()) else {
        eprintln!("usage: serving-server <database file> <address:port>");
        return ExitCode::from(2);
    };
    let addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("serving-server: {addr}: {e}");
            return ExitCode::from(2);
        }
    };
    let token = std::env::var("SERVING_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if token.is_none() && !addr.ip().is_loopback() {
        eprintln!(
            "serving-server: refusing to serve {addr} without a token: set SERVING_TOKEN, and put TLS in front"
        );
        return ExitCode::from(2);
    }
    let config = Config {
        path: PathBuf::from(path),
        lease_ttl: Duration::from_secs(30),
        token,
    };
    // Opening is blocking; it happens once, before serving.
    let app = match tokio::task::spawn_blocking(move || App::open(&config)).await {
        Ok(Ok(app)) => app,
        Ok(Err(e)) => {
            eprintln!("serving-server: {e}");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("serving-server: {e}");
            return ExitCode::FAILURE;
        }
    };
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("serving-server: {addr}: {e}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!("serving-server: serving on {addr}");
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let served = serve(listener, app.clone(), shutdown).await;
    // Close the database cleanly once in-flight requests have finished.
    let closed = tokio::task::spawn_blocking(move || app.close()).await;
    match (served, closed) {
        (Ok(()), Ok(Ok(()))) => ExitCode::SUCCESS,
        (Err(e), _) => {
            eprintln!("serving-server: {e}");
            ExitCode::FAILURE
        }
        (_, Ok(Err(e))) => {
            eprintln!("serving-server: closing: {e}");
            ExitCode::FAILURE
        }
        (_, Err(e)) => {
            eprintln!("serving-server: closing: {e}");
            ExitCode::FAILURE
        }
    }
}
