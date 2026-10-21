//! `phdb backup`: a consistent single-file copy ([`Pigeonhole::backup_durable`], or
//! [`Pigeonhole::backup`] with `--latest`), optionally checked afterwards. While a writer
//! process has the database open, it backs up from a reader process (ICR 0049).
//!
//! [`Pigeonhole::backup_durable`]: pigeonhole::Pigeonhole::backup_durable
//! [`Pigeonhole::backup`]: pigeonhole::Pigeonhole::backup

use std::path::Path;
use std::time::Instant;

use pigeonhole::CheckOptions;
use serde_json::json;

use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::open;
use crate::output::{Column, Report};

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    dest: &Path,
    latest: bool,
    check: bool,
) -> Result<Report, Failure> {
    if dest.exists() {
        return Err(Failure::input(format!(
            "backup: {} exists; the backup writes a new file",
            dest.display()
        )));
    }
    let db = open::for_read(cli, file)?;
    let mode = db.mode();
    let started = Instant::now();
    let copied = db.backup(dest, latest);
    let elapsed = started.elapsed();
    let closed = db.close();
    copied.map_err(|e| Failure::db("backup", &e))?;
    closed?;
    let bytes = std::fs::metadata(dest).map(|m| m.len()).unwrap_or(0);

    let mut summary = vec![
        ("path", json!(file.display().to_string())),
        ("dest", json!(dest.display().to_string())),
        ("mode", json!(mode)),
        ("snapshot", json!(if latest { "latest" } else { "durable" })),
        ("bytes", json!(bytes)),
        ("elapsed_ms", json!(elapsed.as_millis() as u64)),
    ];
    let mut status = ExitStatus::SUCCESS;
    if check {
        let r = pigeonhole::check(dest, CheckOptions::default())
            .map_err(|e| Failure::db("check the backup", &e))?;
        summary.push(("check", json!(if r.is_ok() { "ok" } else { "corrupt" })));
        if !r.is_ok() {
            status = ExitStatus::CORRUPTION;
        }
    }
    let document = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    Ok(Report {
        command: "backup",
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status,
    })
}
