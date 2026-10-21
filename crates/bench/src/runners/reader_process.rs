//! Reader processes beside a measured phase (#683, D247 item K, reported): the run spawns
//! `phdb-bench reader` children that attach to its database with
//! `Pigeonhole::open_reader` and do ycsb-c reads until the run closes their stdin. Each then
//! prints one JSON line with its latency histogram (sparse buckets), misses, errors and
//! reading time, and the run merges them into [`ReaderProcesses`].
//!
//! The children are separate processes, as an application's readers would be: shared memory
//! and the reader slots are exercised for real, and the readers' CPU and cache use compete
//! with the writer's as they would on a host.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::{
    BenchOp, Histogram, LatencyStats, ReaderProcesses, Workload, WorkloadConfig, WorkloadKind,
};

/// Every line a child prints for the run starts with this.
const TAG: &str = "PHREADER ";

/// The environment variable that names the `phdb-bench` binary to spawn, for callers that
/// are not that binary themselves (tests); by default, the running executable.
pub const EXE_ENV: &str = "PHDB_BENCH_EXE";

/// What a child reports when it stops.
#[derive(Debug, Serialize, Deserialize)]
struct ChildReport {
    reads: Vec<(u32, u64)>,
    misses: u64,
    errors: u64,
    secs: f64,
}

/// The children while the measured phase runs.
#[derive(Debug)]
pub(crate) struct Running {
    children: Vec<(Child, Option<ChildStdin>, BufReader<ChildStdout>)>,
}

/// The next tagged line from a child, or an error once its output ends.
fn tagged(out: &mut BufReader<ChildStdout>) -> Result<String, String> {
    let mut line = String::new();
    loop {
        line.clear();
        if out.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
            return Err("a reader process exited without answering".into());
        }
        if let Some(rest) = line.strip_prefix(TAG) {
            return Ok(rest.trim_end().to_owned());
        }
    }
}

/// Spawns `n` reader processes on the database at `db`, reading `config`'s loaded rows,
/// and waits until each has attached and started reading.
pub(crate) fn spawn(db: &Path, n: usize, config: &WorkloadConfig) -> Result<Running, String> {
    let exe = match std::env::var_os(EXE_ENV) {
        Some(p) => PathBuf::from(p),
        None => std::env::current_exe().map_err(|e| e.to_string())?,
    };
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let mut child = Command::new(&exe)
            .arg("reader")
            .arg("--path")
            .arg(db)
            .args(["--records", &config.records.to_string()])
            .args([
                "--seed",
                &(config.seed ^ (0x5EED_0000 + i as u64)).to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawning reader process {}: {e}", exe.display()))?;
        let stdin = child.stdin.take();
        let mut out = BufReader::new(child.stdout.take().expect("piped"));
        let ready = tagged(&mut out)?;
        if ready != "READY" {
            let _ = child.kill();
            return Err(format!("reader process {i}: {ready}"));
        }
        children.push((child, stdin, out));
    }
    Ok(Running { children })
}

impl Running {
    /// Closes every child's stdin, collects their reports and merges them.
    pub(crate) fn stop(mut self) -> Result<ReaderProcesses, String> {
        for (_, stdin, _) in &mut self.children {
            drop(stdin.take());
        }
        let mut hist = Histogram::new();
        let (mut misses, mut errors, mut rate) = (0u64, 0u64, 0f64);
        let processes = self.children.len() as u32;
        for (mut child, _, mut out) in self.children.drain(..) {
            let line = tagged(&mut out)?;
            let status = child.wait().map_err(|e| e.to_string())?;
            if !status.success() {
                return Err(format!("a reader process failed: {status}"));
            }
            let r: ChildReport =
                serde_json::from_str(&line).map_err(|e| format!("reader report {line:?}: {e}"))?;
            let h = Histogram::from_sparse(&r.reads);
            rate += h.count() as f64 / r.secs.max(1e-9);
            hist.merge(&h);
            misses += r.misses;
            errors += r.errors;
        }
        Ok(ReaderProcesses {
            processes,
            reads: LatencyStats::of(&hist),
            reads_per_sec: rate,
            misses,
            errors,
        })
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // A run that failed before `stop` still leaves no reader behind.
        for (child, _, _) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// `phdb-bench reader`: attaches to `path` with `Pigeonhole::open_reader` and reads whole
/// `ycsb` rows, Zipfian over `records` (ycsb-c's distribution, seeded with `seed`), until
/// stdin closes; then prints its report and returns.
pub fn child(path: &Path, records: u64, seed: u64) -> Result<(), String> {
    let say = |line: &str| {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{TAG}{line}");
        let _ = out.flush();
    };
    let reader = pigeonhole::Pigeonhole::open_reader(path, pigeonhole::ReaderOptions::default())
        .map_err(|e| e.to_string())?;
    let table = reader.table("bench").map_err(|e| e.to_string())?;
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            // Until the run closes stdin (or dies, which closes it too).
            let mut sink = Vec::new();
            let _ = std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut sink);
            stop.store(true, Ordering::Release);
        });
    }
    let mut workload = Workload::new(WorkloadConfig {
        kind: WorkloadKind::YcsbC,
        seed,
        records,
        operations: u64::MAX / 4,
        ..WorkloadConfig::smoke(WorkloadKind::YcsbC)
    });
    let (mut hist, mut misses, mut errors) = (Histogram::new(), 0u64, 0u64);
    say("READY");
    let begun = Instant::now();
    for op in workload.run_ops() {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let (row, family) = match &op {
            BenchOp::GetRow { row, family } => (row, *family),
            BenchOp::Get { row, family, .. } => (row, *family),
            _ => continue,
        };
        let t = Instant::now();
        match table.row(row).family(family).read() {
            Ok(Some(_)) => {}
            Ok(None) => misses += 1,
            Err(_) => errors += 1,
        }
        hist.record(t.elapsed());
    }
    let report = ChildReport {
        reads: hist.to_sparse(),
        misses,
        errors,
        secs: begun.elapsed().as_secs_f64(),
    };
    say(&serde_json::to_string(&report).map_err(|e| e.to_string())?);
    Ok(())
}
