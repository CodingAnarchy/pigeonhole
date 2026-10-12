//! The scaling gate's inline driver at smoke size, one and two shards (D204, D223): the runs
//! CI's `smoke_suite_end_to_end` makes. On Linux CI they have intermittently failed with
//! `Busy` after a 30 s write stall, several engines in one process at the same moment (a
//! 1-shard run among them), which points at the runner's disk or something process-wide
//! rather than the driver. 2,000 writes of 100 bytes never fill a 16 MiB memtable, so a
//! stall here is not back-pressure.
//!
//! A probe fsyncs a file on the same disk throughout and a failure prints its slow syncs, so
//! a recurrence tells a stalled disk from a stalled engine. Repeats `PIGEONHOLE_SEEDS` times
//! (once by default) to loop it; the CI failure had two runs in one process at once, so the
//! loop also runs twice concurrently.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pigeonhole_bench::{PigeonholeRunner, RunOptions, WorkloadConfig, WorkloadKind};

fn temp_dir(tag: &str, i: u64) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "phdb-inline-stall-{}-{tag}-{i}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// fsyncs a small file on the same disk every 100 ms while a test runs, recording each sync
/// that took over 100 ms with when it ended, so a failure can tell a stalled disk from a
/// stalled engine.
struct DiskProbe {
    stop: Arc<AtomicBool>,
    worst: Arc<AtomicU64>,
    slow: Arc<Mutex<Vec<(Duration, Duration)>>>,
    started: Instant,
}

impl DiskProbe {
    fn start(tag: &str) -> Self {
        let (stop, worst) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        );
        let slow = Arc::new(Mutex::new(Vec::new()));
        let started = Instant::now();
        let path =
            std::env::temp_dir().join(format!("phdb-disk-probe-{}-{tag}", std::process::id()));
        let (s, w, l) = (Arc::clone(&stop), Arc::clone(&worst), Arc::clone(&slow));
        std::thread::spawn(move || {
            let mut f = std::fs::File::create(&path).unwrap();
            while !s.load(Ordering::Acquire) {
                let t = Instant::now();
                f.write_all(&[7; 4096]).unwrap();
                f.sync_all().unwrap();
                let took = t.elapsed();
                w.fetch_max(took.as_nanos() as u64, Ordering::AcqRel);
                if took > Duration::from_millis(100) {
                    l.lock().unwrap().push((started.elapsed(), took));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            std::fs::remove_file(&path).ok();
        });
        DiskProbe {
            stop,
            worst,
            slow,
            started,
        }
    }

    fn report(&self) -> String {
        format!(
            "disk probe: worst fsync {:?}; fsyncs over 100 ms (ended at, took) since the probe \
             started {:?} ago: {:?}",
            Duration::from_nanos(self.worst.load(Ordering::Acquire)),
            self.started.elapsed(),
            self.slow.lock().unwrap()
        )
    }
}

impl Drop for DiskProbe {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

fn loop_runs(tag: &str) {
    let probe = DiskProbe::start(tag);
    let runs: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let config = WorkloadConfig::smoke(WorkloadKind::SkewedMultiShard);
    let options = RunOptions {
        warmup: 1.0,
        ..RunOptions::default()
    };
    for i in 0..runs {
        for shards in [1, 2] {
            let dir = temp_dir(tag, i);
            let runner = PigeonholeRunner::default()
                .shards(shards)
                .memtable_budget(16 << 20);
            let start = Instant::now();
            let result = runner.run_inline(&config, &dir, &options);
            let took = start.elapsed();
            std::fs::remove_dir_all(&dir).ok();
            if let Err(e) = result {
                panic!(
                    "{tag} run {i}, {shards} shards, after {took:?}: {e}\n{}",
                    probe.report()
                );
            }
            // The warmup waits for 12 s without a tablet change (longer than the balancer's
            // slow dwell, D228), so a healthy run takes about 13-15 s; a table that never
            // spreads waits out the 60 s cap, and a write stall fails with `Busy` at 30 s.
            assert!(
                took < Duration::from_secs(45),
                "{tag} run {i}, {shards} shards took {took:?}\n{}",
                probe.report()
            );
        }
    }
}

#[test]
fn a_two_shard_inline_smoke_run_never_stalls() {
    loop_runs("alone");
}

#[test]
fn two_inline_smoke_runs_at_once_never_stall() {
    std::thread::scope(|s| {
        let a = s.spawn(|| loop_runs("a"));
        let b = s.spawn(|| loop_runs("b"));
        a.join().unwrap();
        b.join().unwrap();
    });
}
