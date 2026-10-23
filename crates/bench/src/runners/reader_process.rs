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
    /// #930 (scratch): the reading thread's CPU time over the loop.
    #[serde(default)]
    cpu_secs: f64,
}

/// This thread's CPU time, in seconds (#930 scratch): utime + stime from
/// `/proc/thread-self/stat`, in 100 Hz clock ticks; 0 where there is no such file.
fn thread_cpu_secs() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/thread-self/stat") else {
        return 0.0;
    };
    // Fields after the parenthesised command name: state is field 3, utime 14, stime 15.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let tick = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    (tick(11) + tick(12)) / 100.0
}

/// How often the writer's reader metrics are sampled while the readers run.
const SAMPLE_EVERY: std::time::Duration = std::time::Duration::from_millis(100);

/// The writer-side maxima: view lag, seqno lag, slots active.
type Lag = (u64, u64, u64);

/// The children while the measured phase runs, and the writer-side sampler.
#[derive(Debug)]
pub(crate) struct Running {
    children: Vec<(Child, Option<ChildStdin>, BufReader<ChildStdout>)>,
    /// #397 measurement (scratch): `PHDB_IN_PROCESS_READERS` threads in the writer process
    /// running the children's read loop, and their stop flag.
    in_process: Vec<std::thread::JoinHandle<ChildReport>>,
    stop_in_process: Arc<AtomicBool>,
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
    let stop_in_process = Arc::new(AtomicBool::new(false));
    let in_process_n: usize = std::env::var("PHDB_IN_PROCESS_READERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut in_process = Vec::new();
    for i in 0..in_process_n {
        let table = writer
            .table("bench")
            .and_then(|b| b.open())
            .map_err(|e| e.to_string())?;
        let stop = Arc::clone(&stop_in_process);
        let (records, seed) = (config.records, config.seed ^ (0x1A_0000 + i as u64));
        in_process.push(std::thread::spawn(move || {
            read_loop(
                |row, family| table.row(row).family(family).read().map(|r| r.is_some()),
                records,
                seed,
                &stop,
            )
        }));
    }
    let stop_sampling = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (writer, stop) = (writer.clone(), Arc::clone(&stop_sampling));
        std::thread::spawn(move || {
            let mut max: Lag = (0, 0, 0);
            loop {
                let m = writer.engine_metrics();
                max.0 = max.0.max(m.reader_pin_view_lag);
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
        in_process,
        stop_in_process,
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
        self.stop_in_process.store(true, Ordering::Release);
        if !self.in_process.is_empty() {
            let mut hist = Histogram::new();
            let (mut misses, mut errors, mut rate) = (0u64, 0u64, 0f64);
            let (mut cpu, mut wall) = (0f64, 0f64);
            for t in self.in_process.drain(..) {
                let r = t
                    .join()
                    .map_err(|_| "an in-process reader panicked".to_owned())?;
                let h = Histogram::from_sparse(&r.reads);
                rate += h.count() as f64 / r.secs.max(1e-9);
                cpu += r.cpu_secs;
                wall += r.secs;
                hist.merge(&h);
                misses += r.misses;
                errors += r.errors;
            }
            let s = LatencyStats::of(&hist);
            eprintln!(
                "INPROC {}",
                serde_json::json!({"reads": s, "reads_per_sec": rate, "misses": misses, "errors": errors, "cpu_ns_per_read": cpu * 1e9 / hist.count().max(1) as f64, "cpu_share": cpu / wall.max(1e-9)})
            );
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
            eprintln!(
                "CHILDCPU {}",
                serde_json::json!({"cpu_ns_per_read": r.cpu_secs * 1e9 / h.count().max(1) as f64, "cpu_share": r.cpu_secs / r.secs.max(1e-9), "reads_per_sec": h.count() as f64 / r.secs.max(1e-9)})
            );
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
        self.stop_in_process.store(true, Ordering::Release);
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
    say("READY");
    let report = read_loop(
        |row, family| table.row(row).family(family).read().map(|r| r.is_some()),
        records,
        seed,
        &stop,
    );
    say(&serde_json::to_string(&report).map_err(|e| e.to_string())?);
    Ok(())
}

/// The read loop of a reader process, and of an in-process reader (#397 measurement):
/// whole `ycsb` rows, Zipfian over `records` (ycsb-c's distribution, seeded with `seed`),
/// or, with `PHDB_READER_HOT=K`, uniformly over the K hottest rows of that distribution (the
/// rows ycsb-a's updates overwrite most), until `stop`.
fn read_loop<E>(
    mut read: impl FnMut(&[u8], &str) -> Result<bool, E>,
    records: u64,
    seed: u64,
    stop: &AtomicBool,
) -> ChildReport {
    let hot: u64 = std::env::var("PHDB_READER_HOT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let hot_rows: Vec<Vec<u8>> = (0..hot)
        .map(|r| crate::workload::ycsb_key(crate::rng::fnv64(r) % records.max(1)))
        .collect();
    let mut rng = crate::rng::Rng::new(seed);
    let mut workload = Workload::new(WorkloadConfig {
        kind: WorkloadKind::YcsbC,
        seed,
        records,
        operations: u64::MAX / 4,
        ..WorkloadConfig::smoke(WorkloadKind::YcsbC)
    });
    let mut ops = workload.run_ops();
    let (mut hist, mut misses, mut errors) = (Histogram::new(), 0u64, 0u64);
    let begun = Instant::now();
    let cpu0 = thread_cpu_secs();
    while !stop.load(Ordering::Acquire) {
        let (row, family) = if hot_rows.is_empty() {
            match ops.next() {
                Some(BenchOp::GetRow { row, family }) | Some(BenchOp::Get { row, family, .. }) => {
                    (row, family)
                }
                Some(_) => continue,
                None => break,
            }
        } else {
            (
                hot_rows[rng.below(hot) as usize].clone(),
                crate::workload::YCSB_FAMILY,
            )
        };
        let t = Instant::now();
        match read(&row, family) {
            Ok(true) => {}
            Ok(false) => misses += 1,
            Err(_) => errors += 1,
        }
        hist.record(t.elapsed());
    }
    ChildReport {
        reads: hist.to_sparse(),
        misses,
        errors,
        secs: begun.elapsed().as_secs_f64(),
        cpu_secs: thread_cpu_secs() - cpu0,
    }
}
