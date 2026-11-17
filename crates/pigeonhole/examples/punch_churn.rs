//! Box-only (scratch/perf2-2034-ab, g152): a delete- and compaction-heavy store, to show
//! whether D333's punching gives the freed blocks back. Never merged.
//!
//! Phases: `load` (rows of incompressible values), `delete` (the first `--delete-pct`% of the
//! rows by key, then a full `compact()`), `steady` (paced Buffered puts, half overwrites, and gets:
//! row 1 measures the sync tail, this one punching's cost to the engine itself), with an idle wait (no flush or compaction for 5 s, at most 600 s) after load and delete.
//!
//! Every second it prints one line:
//! `S <epoch_ms> phase=<load|load-idle|delete|delete-idle|steady|done> apparent=<bytes>
//! allocated=<bytes> file_punches=N file_punch_nanos=N file_punch_bytes=N
//! file_punched_bytes=N file_punch_passes=N file_punch_refusals=N flushes=N compactions=N`
//! (`allocated` is `st_blocks * 512`). At start: `START memtable_limit_bytes=N
//! punch_freed=<bool>` (the reserve is min(max(64 MiB, 2 x that), file/4)); after each phase: `PHASE <name> end_ms=<epoch_ms> secs=<f>`; at the
//! end: `STEADY puts=N gets=N put_p50_us= put_p99_us= put_p999_us= get_p50_us= get_p99_us=
//! get_p999_us= late_max_us=` (latency from each op's scheduled time, open loop).
//!
//! `cargo run --release -p pigeonhole --features __unstable-hooks --example punch_churn --
//!  --dir D [--punch-freed] [--rows 4194304] [--value 2048] [--delete-pct 75]
//!  [--steady-secs 300] [--rate 20000]`

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pigeonhole::{Durability, Family, Options, Pigeonhole};

struct Args {
    dir: PathBuf,
    punch: bool,
    rows: u64,
    value: usize,
    delete_pct: u64,
    steady_secs: u64,
    rate: u64,
}

fn args() -> Args {
    let mut a = Args {
        dir: PathBuf::new(),
        punch: false,
        rows: 4 << 20,
        value: 2048,
        delete_pct: 75,
        steady_secs: 300,
        rate: 20_000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(f) = it.next() {
        let mut v = || it.next().expect("a flag's value");
        match f.as_str() {
            "--dir" => a.dir = PathBuf::from(v()),
            "--punch-freed" => a.punch = true,
            "--rows" => a.rows = v().parse().expect("--rows"),
            "--value" => a.value = v().parse().expect("--value"),
            "--delete-pct" => a.delete_pct = v().parse().expect("--delete-pct"),
            "--steady-secs" => a.steady_secs = v().parse().expect("--steady-secs"),
            "--rate" => a.rate = v().parse().expect("--rate"),
            _ => panic!("unknown flag {f}"),
        }
    }
    assert!(!a.dir.as_os_str().is_empty(), "--dir is required");
    a
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

fn key(i: u64) -> [u8; 9] {
    let mut k = [b'r'; 9];
    k[1..].copy_from_slice(&i.to_be_bytes());
    k
}

/// Incompressible bytes, different per (row, version).
fn fill(buf: &mut [u8], mut s: u64) {
    s |= 1;
    for c in buf.chunks_mut(8) {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let b = s.to_le_bytes();
        c.copy_from_slice(&b[..c.len()]);
    }
}

fn wait_idle(db: &Pigeonhole) {
    let start = Instant::now();
    let mut last = (u64::MAX, u64::MAX);
    let mut since = Instant::now();
    while start.elapsed() < Duration::from_secs(600) {
        let m = db.engine_metrics();
        let now = (m.flushes, m.compactions);
        if now != last {
            last = now;
            since = Instant::now();
        } else if since.elapsed() >= Duration::from_secs(5) {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 * p).ceil() as usize).clamp(1, v.len()) - 1;
    v[i] as f64 / 1000.0
}

fn main() -> pigeonhole::Result<()> {
    let a = args();
    std::fs::create_dir_all(&a.dir).expect("create --dir");
    let path = a.dir.join("churn.phdb");
    let db = Pigeonhole::open(&path, Options::default().punch_freed(a.punch))?;
    let t = db
        .table("t")?
        .family("d", Family::default().max_versions(1))
        .create_if_missing()?;
    let m = db.engine_metrics();
    println!(
        "START memtable_limit_bytes={} punch_freed={}",
        m.memtable_limit_bytes, a.punch
    );
    let phase = Mutex::new("load");
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| sampler(&db, &path, &phase, &stop));
        let r = run(&a, &db, &t, &phase);
        *phase.lock().unwrap() = "done";
        std::thread::sleep(Duration::from_millis(1100));
        stop.store(true, Ordering::Relaxed);
        r
    })?;
    db.close()
}

fn sampler(db: &Pigeonhole, path: &Path, phase: &Mutex<&'static str>, stop: &AtomicBool) {
    let mut next = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let (apparent, allocated) = std::fs::metadata(path)
            .map(|md| (md.len(), md.blocks() * 512))
            .unwrap_or((0, 0));
        let mut line = format!(
            "S {} phase={} apparent={apparent} allocated={allocated}",
            now_ms(),
            *phase.lock().unwrap()
        );
        for (n, v) in db.internal_metrics() {
            if n.starts_with("file_punch") {
                line.push_str(&format!(" {n}={v}"));
            }
        }
        let m = db.engine_metrics();
        line.push_str(&format!(" flushes={} compactions={}", m.flushes, m.compactions));
        println!("{line}");
        next += Duration::from_secs(1);
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
}

fn run(
    a: &Args,
    db: &Pigeonhole,
    t: &pigeonhole::Table,
    phase: &Mutex<&'static str>,
) -> pigeonhole::Result<()> {
    let set = |p: &'static str| *phase.lock().unwrap() = p;
    let mut val = vec![0u8; a.value];
    let mut t0 = Instant::now();
    let end = |name: &str, t0: Instant| {
        println!(
            "PHASE {name} end_ms={} secs={:.1}",
            now_ms(),
            t0.elapsed().as_secs_f64()
        )
    };

    // Load.
    for chunk in (0..a.rows).collect::<Vec<_>>().chunks(256) {
        let mut wb = db.write_batch();
        for &i in chunk {
            fill(&mut val, i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
            wb.put(t, &key(i), "d", b"v", &val);
        }
        wb.commit_with(Durability::Buffered)?;
    }
    end("load", t0);
    set("load-idle");
    t0 = Instant::now();
    wait_idle(db);
    end("load-idle", t0);

    // Delete the first delete_pct% by key, then compact everything.
    set("delete");
    t0 = Instant::now();
    let cut = a.rows * a.delete_pct / 100;
    for chunk in (0..cut).collect::<Vec<_>>().chunks(1024) {
        let mut wb = db.write_batch();
        for &i in chunk {
            wb.delete_row(t, &key(i));
        }
        wb.commit_with(Durability::Buffered)?;
    }
    db.compact()?;
    end("delete", t0);
    set("delete-idle");
    t0 = Instant::now();
    wait_idle(db);
    end("delete-idle", t0);

    // Steady: open loop at `rate` ops/s, alternating put and get over the live rows;
    // half the puts overwrite a live row, half write a new one past the end.
    set("steady");
    t0 = Instant::now();
    let live = a.rows - cut;
    let (mut puts, mut gets) = (Vec::new(), Vec::new());
    let mut late_max = Duration::ZERO;
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    let mut fresh = a.rows;
    let total = a.rate * a.steady_secs;
    for n in 0..total {
        let due = t0 + Duration::from_nanos(n * 1_000_000_000 / a.rate);
        let now = Instant::now();
        if due > now {
            std::thread::sleep(due - now);
        } else {
            late_max = late_max.max(now - due);
        }
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let row = cut + rng % live;
        if n % 2 == 0 {
            let k = if n % 4 == 0 {
                row
            } else {
                fresh += 1;
                fresh
            };
            fill(&mut val, k ^ n);
            t.mutate(&key(k))
                .put("d", b"v", &val)
                .durability(Durability::Buffered)
                .commit()?;
            puts.push(due.elapsed().as_nanos() as u64);
        } else {
            let _ = t.get(&key(row), "d", b"v")?;
            gets.push(due.elapsed().as_nanos() as u64);
        }
    }
    end("steady", t0);
    println!(
        "STEADY puts={} gets={} put_p50_us={:.1} put_p99_us={:.1} put_p999_us={:.1} get_p50_us={:.1} get_p99_us={:.1} get_p999_us={:.1} late_max_us={}",
        puts.len(),
        gets.len(),
        pct(&mut puts, 0.5),
        pct(&mut puts, 0.99),
        pct(&mut puts, 0.999),
        pct(&mut gets, 0.5),
        pct(&mut gets, 0.99),
        pct(&mut gets, 0.999),
        late_max.as_micros()
    );
    Ok(())
}
