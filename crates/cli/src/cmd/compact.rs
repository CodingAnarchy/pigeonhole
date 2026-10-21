//! `phdb compact`: a full compaction ([`Pigeonhole::compact`](pigeonhole::Pigeonhole::compact))
//! with progress on standard error, then optionally
//! [`Pigeonhole::shrink`](pigeonhole::Pigeonhole::shrink).
//!
//! The compaction API reports no progress, so phdb runs it on a thread and polls the engine's
//! counters (flushes and compactions finished), printing them every half second. Progress prints only with
//! `--format table`: the other formats are for programs reading stdout, and keep stderr
//! for errors.

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::ExitStatus;
use crate::args::{Cli, Format};
use crate::exit::Failure;
use crate::open;
use crate::output::{Column, Report};

const POLL: Duration = Duration::from_millis(500);

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    shrink: bool,
    stderr: &mut dyn Write,
) -> Result<Report, Failure> {
    let db = open::for_write(cli, file, false)?;
    let progress = cli.format() == Format::Table;
    let size = || std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
    let bytes_before = size();
    let before = db.engine_metrics();
    let started = Instant::now();
    let compacted = std::thread::scope(|s| {
        let worker = s.spawn(|| db.compact());
        let mut printed = started;
        while !worker.is_finished() {
            std::thread::sleep(Duration::from_millis(20));
            if progress && printed.elapsed() >= POLL && !worker.is_finished() {
                printed = Instant::now();
                let m = db.engine_metrics();
                let _ = writeln!(
                    stderr,
                    "compacting: {:.1} s, {} flushes, {} compactions done",
                    started.elapsed().as_secs_f64(),
                    m.flushes - before.flushes,
                    m.compactions - before.compactions,
                );
            }
        }
        worker.join().expect("the compaction thread does not panic")
    });
    let elapsed = started.elapsed();
    let after = db.engine_metrics();
    let result = compacted
        .map_err(|e| Failure::db("compact", &e))
        .and_then(|()| {
            if shrink {
                db.shrink().map(Some).map_err(|e| Failure::db("shrink", &e))
            } else {
                Ok(None)
            }
        });
    let free = db.info().ok().and_then(|i| i.free_bytes);
    let closed = db.close().map_err(|e| Failure::db("close", &e));
    let released = result?;
    closed?;

    let summary = vec![
        ("path", json!(file.display().to_string())),
        ("elapsed_ms", json!(elapsed.as_millis() as u64)),
        ("flushes", json!(after.flushes - before.flushes)),
        ("compactions", json!(after.compactions - before.compactions)),
        ("file_bytes_before", json!(bytes_before)),
        ("released_bytes", json!(released)),
        ("file_bytes", json!(size())),
        ("free_bytes", json!(free)),
    ];
    let document = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    Ok(Report {
        command: "compact",
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status: ExitStatus::SUCCESS,
    })
}
