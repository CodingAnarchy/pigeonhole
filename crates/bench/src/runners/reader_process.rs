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

/// How often the writer's reader metrics are sampled while the readers run.
const SAMPLE_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

/// The writer-side maxima: view lag, seqno lag, slots active.
type Lag = (u64, u64, u64);

/// The children while the measured phase runs, and the writer-side sampler.
#[derive(Debug)]
pub(crate) struct Running {
    children: Vec<(Child, Option<ChildStdin>, BufReader<ChildStdout>)>,
    stop_sampling: Arc<AtomicBool>,
    sampler: Option<std::thread::JoinHandle<Lag>>,
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

/// Spawns `n` reader processes on the database at `db` (open in this process as `writer`),
/// reading `config`'s loaded rows, waits until each has attached and started reading, and
/// samples the writer's reader metrics until [`Running::stop`].
pub(crate) fn spawn(
    writer: &pigeonhole::Pigeonhole,
    db: &Path,
    n: usize,
    config: &WorkloadConfig,
) -> Result<Running, String> {
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
    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (writer, stop) = (writer.clone(), Arc::clone(&stop_sampling));
        std::thread::spawn(move || {
            let mut max: Lag = (0, 0, 0);
            loop {
                let m = writer.engine_metrics();
                let view_lag =
                    super::pigeonhole::internal(&writer.internal_metrics(), "reader_pin_view_lag");
                max.0 = max.0.max(view_lag);
                max.1 = max.1.max(m.reader_pin_seqno_lag);
                max.2 = max.2.max(m.reader_slots_active);
                if stop.load(Ordering::Acquire) {
                    return max;
                }
                std::thread::sleep(SAMPLE_EVERY);
            }
        })
    };
    Ok(Running {
        children,
        stop_sampling,
        sampler: Some(sampler),
    })
}

impl Running {
    /// Closes every child's stdin, collects their reports and merges them.
    pub(crate) fn stop(mut self) -> Result<ReaderProcesses, String> {
        // The last sample is taken with the readers still attached.
        self.stop_sampling.store(true, Ordering::Release);
        let (max_view_lag, max_seqno_lag, max_slots_active) = self
            .sampler
            .take()
            .map_or((0, 0, 0), |s| s.join().unwrap_or((0, 0, 0)));
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
            max_view_lag,
            max_seqno_lag,
            max_slots_active,
        })
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.stop_sampling.store(true, Ordering::Release);
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

/// What a `reader --feed` child reports when it stops (D310 PR 5b).
#[derive(Debug, Serialize, Deserialize)]
struct FeedChildReport {
    delivered: Vec<(u32, u64)>,
    expired: u64,
    errors: u64,
    /// 1 if the drain's deadline stopped it short of the parent's seqno.
    #[serde(default)]
    drain_timeouts: u64,
    /// [`Followed::error_messages`](super::feed::Followed::error_messages).
    #[serde(default)]
    error_messages: Vec<String>,
    secs: f64,
    /// User + system CPU seconds over the run (Linux), `None` elsewhere.
    cpu_secs: Option<f64>,
    /// Voluntary context switches over the run (Linux), `None` elsewhere.
    wakeups: Option<u64>,
}

/// The `reader --feed` children, merged when they stop.
#[derive(Debug)]
pub(crate) struct FeedChildren {
    children: Vec<(Child, Option<ChildStdin>, BufReader<ChildStdout>)>,
}

/// What [`FeedChildren::stop`] merged.
#[derive(Debug)]
pub(crate) struct FeedChildrenReport {
    pub(crate) processes: u32,
    pub(crate) hist: Histogram,
    pub(crate) expired: u64,
    pub(crate) errors: u64,
    pub(crate) drain_timeouts: u64,
    pub(crate) error_messages: Vec<String>,
    pub(crate) cpu_per_sec: Option<f64>,
    pub(crate) wakeups_per_sec: Option<f64>,
}

/// Spawns `n` `phdb-bench reader --feed` children on the database at `db` and waits until
/// each has its feed open.
pub(crate) fn spawn_feed(db: &Path, n: usize) -> Result<FeedChildren, String> {
    let exe = match std::env::var_os(EXE_ENV) {
        Some(p) => PathBuf::from(p),
        None => std::env::current_exe().map_err(|e| e.to_string())?,
    };
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let mut child = Command::new(&exe)
            .arg("reader")
            .arg("--feed")
            .arg("--path")
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawning feed reader {}: {e}", exe.display()))?;
        let stdin = child.stdin.take();
        let mut out = BufReader::new(child.stdout.take().expect("piped"));
        let ready = tagged(&mut out)?;
        if ready != "READY" {
            let _ = child.kill();
            return Err(format!("feed reader {i}: {ready}"));
        }
        children.push((child, stdin, out));
    }
    Ok(FeedChildren { children })
}

impl FeedChildren {
    /// Tells every child to stop once its feed covers `through` (bounded by
    /// [`DRAIN_WAIT`](super::feed::DRAIN_WAIT)): the seqno on its stdin, which then closes.
    pub(crate) fn drain(&mut self, through: u64) {
        for (_, stdin, _) in &mut self.children {
            if let Some(mut s) = stdin.take() {
                // A child that already exited reports anyway, or fails `stop`.
                let _ = writeln!(s, "{through}");
            }
        }
    }

    /// Closes every child's stdin (a child not told to [`drain`](Self::drain) stops at
    /// once) and merges their reports.
    pub(crate) fn stop(mut self) -> Result<FeedChildrenReport, String> {
        for (_, stdin, _) in &mut self.children {
            drop(stdin.take());
        }
        let mut out = FeedChildrenReport {
            processes: self.children.len() as u32,
            hist: Histogram::new(),
            expired: 0,
            errors: 0,
            drain_timeouts: 0,
            error_messages: Vec::new(),
            cpu_per_sec: Some(0.0),
            wakeups_per_sec: Some(0.0),
        };
        for (mut child, _, mut lines) in self.children.drain(..) {
            let line = tagged(&mut lines)?;
            let status = child.wait().map_err(|e| e.to_string())?;
            if !status.success() {
                return Err(format!("a feed reader failed: {status}"));
            }
            let r: FeedChildReport = serde_json::from_str(&line)
                .map_err(|e| format!("feed reader report {line:?}: {e}"))?;
            out.hist.merge(&Histogram::from_sparse(&r.delivered));
            out.expired += r.expired;
            out.errors += r.errors;
            out.drain_timeouts += r.drain_timeouts;
            out.error_messages.extend(r.error_messages);
            let secs = r.secs.max(1e-9);
            out.cpu_per_sec = out.cpu_per_sec.zip(r.cpu_secs).map(|(a, c)| a + c / secs);
            out.wakeups_per_sec = out
                .wakeups_per_sec
                .zip(r.wakeups)
                .map(|(a, w)| a + w as f64 / secs);
        }
        Ok(out)
    }
}

impl Drop for FeedChildren {
    fn drop(&mut self) {
        for (child, _, _) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// This process's user + system CPU seconds (Linux: `/proc/self/stat` fields 14 and 15, in
/// clock ticks of 100 Hz, the kernel's `USER_HZ` on every Linux the bench runs on).
fn cpu_secs() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Fields after the command name, which is in parentheses and may hold spaces.
    let rest = stat.rsplit_once(") ")?.1;
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f.get(11)?.parse().ok()?;
    let stime: u64 = f.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / 100.0)
}

/// This process's voluntary context switches, every thread (Linux: `/proc/self/status` counts
/// the main thread only, so the threads' own status files are summed).
fn wakeups() -> Option<u64> {
    let mut total = 0;
    for task in std::fs::read_dir("/proc/self/task").ok()? {
        let status = std::fs::read_to_string(task.ok()?.path().join("status")).ok()?;
        let line = status
            .lines()
            .find(|l| l.starts_with("voluntary_ctxt_switches:"))?;
        total += line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    }
    Some(total)
}

/// `phdb-bench reader --feed`: attaches to `path` with `Pigeonhole::open_reader`, follows the
/// `bench` table's durable feed from now until stdin closes (and its feed covers the seqno
/// written there, if any, for at most 30 s), then prints
/// its report: the
/// delivery latencies (wall clock minus each commit's timestamp), and its CPU and wakeups
/// over the run, so a backoff's idle cost is measured beside its latency.
pub fn child_feed(path: &Path) -> Result<(), String> {
    let say = |line: &str| {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{TAG}{line}");
        let _ = out.flush();
    };
    let reader = pigeonhole::Pigeonhole::open_reader(path, pigeonhole::ReaderOptions::default())
        .map_err(|e| e.to_string())?;
    let table = reader.table("bench").map_err(|e| e.to_string())?;
    let open = || table.feed().open();
    let feed = open().map_err(|e| e.to_string())?;
    let stop = Arc::new(super::feed::Stop::default());
    {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut sink = String::new();
            let _ = std::io::Read::read_to_string(&mut std::io::stdin().lock(), &mut sink);
            stop.drain(sink.trim().parse().unwrap_or(0));
        });
    }
    let (cpu0, wake0) = (cpu_secs(), wakeups());
    say("READY");
    let begun = Instant::now();
    let followed = super::feed::follow(feed, open, &stop);
    let secs = begun.elapsed().as_secs_f64();
    let report = FeedChildReport {
        delivered: followed.hist.to_sparse(),
        expired: followed.expired,
        errors: followed.errors,
        drain_timeouts: followed.drain_timeouts,
        error_messages: followed.error_messages,
        secs,
        cpu_secs: cpu0.zip(cpu_secs()).map(|(a, b)| b - a),
        wakeups: wake0.zip(wakeups()).map(|(a, b)| b.saturating_sub(a)),
    };
    say(&serde_json::to_string(&report).map_err(|e| e.to_string())?);
    Ok(())
}
