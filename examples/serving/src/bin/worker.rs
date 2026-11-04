//! `serving-worker`: claims and runs queued jobs until none are left.
//!
//! ```sh
//! cargo run -p pigeonhole-example-serving --bin serving-worker -- http://127.0.0.1:7070 worker-1
//! ```
//!
//! Run as many as you like, on any host that can reach the server: each job is run by one of
//! them, and a worker that stalls past its claim's TTL can't overwrite the job's result.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use pigeonhole_example_serving::Client;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let (Some(base), Some(worker)) = (args.next(), args.next()) else {
        eprintln!("usage: serving-worker <server URL> <worker name>");
        return ExitCode::from(2);
    };
    let client = Client::new(
        base,
        std::env::var("SERVING_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    );
    // The "work" is a stand-in: the result names the worker that did it.
    match client.work_until_idle(&worker, |id| format!("{id} done by {worker}")) {
        Ok(done) => {
            println!("{worker}: completed {} jobs", done.len());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{worker}: {e}");
            ExitCode::FAILURE
        }
    }
}
