//! A writer killed at random points, over and over, on real files: every commit the writer
//! acknowledged survives the kill, and nothing appears that it didn't make.
//!
//! Each cycle re-executes this test binary as a writer child. Two threads in it commit
//! batches of [`ROWS`] rows to keys of their own, at `Buffered`, `GroupSync`, or a mix with
//! `Sync`, and print `ACK <thread> <batch>` once each commit returns. The parent SIGKILLs it
//! (`TerminateProcess` on Windows) after a random delay, sometimes before it has finished
//! opening, so recovery gets killed too. Then it reopens the database and checks, per thread:
//! - every batch is whole: all its rows or none;
//! - the batches present are a prefix, `0..n`, with no holes (each thread commits one at a
//!   time, and a process kill keeps everything a returned `Buffered` commit wrote);
//! - `n` covers every acknowledged batch, and is at most one past the last acknowledged (the
//!   commit in flight);
//! - every value is the one written, and the batches of earlier cycles are all still there.
//!
//! The I/O backend is drawn per cycle: `Pread`, and on Linux also `Uring`. A process kill
//! keeps the OS page cache, so this checks the process-crash promise, not power loss (the
//! simulator's model checks cover that).
//!
//! `PIGEONHOLE_KILL_CYCLES` sets the cycles (10 by default, so CI runs a few);
//! `PIGEONHOLE_SEED` replays a run, whose seed every failure prints. A long run:
//! `PIGEONHOLE_KILL_CYCLES=3000 cargo test --release -p pigeonhole --test kill_loop -- --nocapture`.
#![cfg(not(miri))]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use pigeonhole::{Durability, Family, IoBackend, Options, Pigeonhole, Table};

const CHILD_ENV: &str = "PIGEONHOLE_KILL_LOOP_CHILD";
/// Every line a child prints for the parent starts with this.
const TAG: &str = "PHKILL ";
/// Rows per batch (one commit).
const ROWS: u64 = 8;
/// Writer threads in the child.
const THREADS: u64 = 2;
/// A fresh database every so many cycles, so the per-cycle check stays short.
const CYCLES_PER_DB: u64 = 200;

fn options(backend: IoBackend) -> Options {
    Options::default()
        .shards(2)
        .pin_threads(false)
        .io_backend(backend)
        .memtable_budget(4 << 20)
        .block_cache(4 << 20)
}

fn table(db: &Pigeonhole) -> Table {
    db.table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap()
}

fn key(cycle: u64, thread: u64, batch: u64, row: u64) -> String {
    format!("c{cycle:06}-t{thread}-b{batch:08}-r{row}")
}

/// The value of a batch's row: its batch number, then padding.
fn value(batch: u64) -> Vec<u8> {
    let mut v = batch.to_le_bytes().to_vec();
    v.resize(256, (batch % 251) as u8);
    v
}

/// A small seeded generator (splitmix64).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Buffered,
    GroupSync,
    /// Each commit draws `Buffered`, `GroupSync` or `Sync`.
    Mixed,
}

fn backend_name(b: IoBackend) -> &'static str {
    match b {
        IoBackend::Pread => "pread",
        IoBackend::Uring => "uring",
        _ => "auto",
    }
}

fn parse_backend(s: &str) -> IoBackend {
    match s {
        "pread" => IoBackend::Pread,
        "uring" => IoBackend::Uring,
        _ => IoBackend::Auto,
    }
}

/// Runs in a child process. Without the environment variable it is an empty test.
#[test]
fn child_main() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let parts: Vec<&str> = spec.split('|').collect();
    let [path, cycle, backend, mode, seed] = parts[..] else {
        panic!("bad child spec {spec:?}");
    };
    let cycle: u64 = cycle.parse().unwrap();
    let seed: u64 = seed.parse().unwrap();
    let mode = match mode {
        "buffered" => Mode::Buffered,
        "groupsync" => Mode::GroupSync,
        _ => Mode::Mixed,
    };
    let db = Pigeonhole::open(path, options(parse_backend(backend))).unwrap();
    let t = table(&db);
    let out = Mutex::new(std::io::stdout());
    let say = |line: String| {
        let mut o = out.lock().unwrap();
        writeln!(o, "{TAG}{line}").unwrap();
        o.flush().unwrap();
    };
    say("READY".into());
    std::thread::scope(|s| {
        for thread in 0..THREADS {
            let (db, t, say) = (&db, &t, &say);
            s.spawn(move || {
                let mut rng = Rng(seed ^ (thread + 1).wrapping_mul(0x5851_f42d_4c95_7f2d));
                // Until killed: far more batches than any cycle reaches.
                for batch in 0..u64::MAX {
                    let mut wb = db.write_batch();
                    for row in 0..ROWS {
                        wb.put(
                            t,
                            key(cycle, thread, batch, row).as_bytes(),
                            "f",
                            b"v",
                            &value(batch),
                        );
                    }
                    let durability = match mode {
                        Mode::Buffered => Durability::Buffered,
                        Mode::GroupSync => Durability::GroupSync,
                        Mode::Mixed => match rng.below(3) {
                            0 => Durability::Buffered,
                            1 => Durability::GroupSync,
                            _ => Durability::Sync,
                        },
                    };
                    match wb.commit_with(durability) {
                        Ok(_) => say(format!("ACK {thread} {batch}")),
                        Err(e) => {
                            say(format!("ERR {thread} {batch} {:?} {e}", e.code()));
                            return;
                        }
                    }
                }
            });
        }
    });
}

/// What one cycle's child acknowledged before it died.
struct Acks {
    ready: bool,
    /// The last acknowledged batch per thread, if any.
    last: [Option<u64>; THREADS as usize],
    errors: Vec<String>,
    /// The child exited before the kill: it failed (it never exits on its own).
    exited: Option<String>,
}

/// Spawns the child, kills it after `delay`, and collects what it acknowledged.
fn run_child(
    path: &Path,
    cycle: u64,
    backend: IoBackend,
    mode: Mode,
    seed: u64,
    delay: Duration,
) -> Acks {
    let mode = match mode {
        Mode::Buffered => "buffered",
        Mode::GroupSync => "groupsync",
        Mode::Mixed => "mixed",
    };
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_main", "--nocapture", "--test-threads=1"])
        .env(
            CHILD_ENV,
            format!(
                "{}|{cycle}|{}|{mode}|{seed}",
                path.display(),
                backend_name(backend)
            ),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let stderr = child.stderr.take().unwrap();
    // Read both pipes to their ends on threads of their own: after the kill they end, and
    // every line the child printed before it is still in the pipe.
    let lines = std::thread::spawn(move || {
        stdout
            .lines()
            .map_while(Result::ok)
            .filter_map(|l| {
                l.find(TAG)
                    .map(|p| l[p + TAG.len()..].trim_end().to_owned())
            })
            .collect::<Vec<_>>()
    });
    let errs = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = std::io::Read::read_to_string(&mut BufReader::new(stderr), &mut s);
        s
    });
    std::thread::sleep(delay);
    let exited = child.try_wait().unwrap();
    let _ = child.kill();
    child.wait().unwrap();
    let lines = lines.join().unwrap();
    let stderr = errs.join().unwrap();
    let mut acks = Acks {
        ready: false,
        last: [None; THREADS as usize],
        errors: Vec::new(),
        exited: exited.map(|s| {
            format!(
                "{s}: {}",
                stderr.lines().rev().take(20).collect::<Vec<_>>().join("\n")
            )
        }),
    };
    for l in lines {
        let w: Vec<&str> = l.split_whitespace().collect();
        match w[..] {
            ["READY"] => acks.ready = true,
            ["ACK", t, b] => {
                let (t, b): (usize, u64) = (t.parse().unwrap(), b.parse().unwrap());
                acks.last[t] = Some(acks.last[t].map_or(b, |x| x.max(b)));
            }
            _ => acks.errors.push(l),
        }
    }
    acks
}

/// The batches present per (cycle, thread), checked whole and correct; `Err` names the
/// first violation.
fn recovered(db: &Pigeonhole) -> Result<BTreeMap<(u64, u64), u64>, String> {
    let t = table(db);
    // (cycle, thread, batch) -> rows seen.
    let mut rows: BTreeMap<(u64, u64, u64), u64> = BTreeMap::new();
    for row in t
        .scan_prefix(b"c")
        .iter()
        .map_err(|e| format!("scan: {e}"))?
    {
        let row = row.map_err(|e| format!("scan: {e}"))?;
        let k = std::str::from_utf8(row.key()).map_err(|_| "a key that isn't UTF-8".to_owned())?;
        let p: Vec<&str> = k.split('-').collect();
        let num = |s: &str, prefix: char| -> Result<u64, String> {
            s.strip_prefix(prefix)
                .and_then(|n| n.parse().ok())
                .ok_or_else(|| format!("unexpected key {k:?}"))
        };
        let [c, th, b, _r] = p[..] else {
            return Err(format!("unexpected key {k:?}"));
        };
        let (c, th, b) = (num(c, 'c')?, num(th, 't')?, num(b, 'b')?);
        let cell = row.get("f", b"v").ok_or_else(|| format!("{k}: no value"))?;
        if cell.value() != value(b).as_slice() {
            return Err(format!("{k}: wrong value"));
        }
        *rows.entry((c, th, b)).or_default() += 1;
    }
    let mut present: BTreeMap<(u64, u64), Vec<u64>> = BTreeMap::new();
    for ((c, th, b), n) in rows {
        if n != ROWS {
            return Err(format!(
                "cycle {c} thread {th} batch {b}: {n} of {ROWS} rows (a torn commit)"
            ));
        }
        present.entry((c, th)).or_default().push(b);
    }
    let mut counts = BTreeMap::new();
    for ((c, th), batches) in present {
        let n = batches.len() as u64;
        if batches.iter().enumerate().any(|(i, &b)| b != i as u64) {
            let hole = batches
                .iter()
                .enumerate()
                .find(|(i, b)| **b != *i as u64)
                .unwrap()
                .0;
            return Err(format!(
                "cycle {c} thread {th}: batch {hole} missing below batch {}",
                batches[batches.len() - 1]
            ));
        }
        counts.insert((c, th), n);
    }
    Ok(counts)
}

fn seed() -> u64 {
    std::env::var("PIGEONHOLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        })
}

#[test]
fn acknowledged_commits_survive_a_writer_killed_at_random_points() {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let cycles: u64 = std::env::var("PIGEONHOLE_KILL_CYCLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let seed = seed();
    eprintln!("kill loop: seed {seed}, {cycles} cycles (PIGEONHOLE_SEED replays it)");
    let mut rng = Rng(seed);
    let root = std::env::temp_dir().join(format!("pigeonhole-kill-{}-{seed}", std::process::id()));
    let backends: &[IoBackend] = if cfg!(target_os = "linux") {
        &[IoBackend::Pread, IoBackend::Uring]
    } else {
        &[IoBackend::Pread]
    };
    // Every (cycle, thread) checked so far and how many batches it had: a later cycle must
    // still find exactly those.
    let mut expected: BTreeMap<(u64, u64), u64> = BTreeMap::new();
    let mut dir: Option<PathBuf> = None;
    let started = Instant::now();
    let (mut acked_total, mut killed_before_ready) = (0u64, 0u64);
    for cycle in 0..cycles {
        if cycle % CYCLES_PER_DB == 0 {
            if let Some(d) = dir.take() {
                let _ = std::fs::remove_dir_all(&d);
            }
            let d = root.join(format!("db{cycle}"));
            std::fs::create_dir_all(&d).unwrap();
            dir = Some(d);
            expected.clear();
        }
        let path = dir.as_ref().unwrap().join("data.phdb");
        let backend = backends[rng.below(backends.len() as u64) as usize];
        let mode = match rng.below(3) {
            0 => Mode::Buffered,
            1 => Mode::GroupSync,
            _ => Mode::Mixed,
        };
        // Mostly while it commits; one cycle in eight early, often during the open.
        let delay = if rng.below(8) == 0 {
            Duration::from_micros(rng.below(30_000))
        } else {
            Duration::from_micros(30_000 + rng.below(120_000))
        };
        let child_seed = rng.next();
        let acks = run_child(&path, cycle, backend, mode, child_seed, delay);
        let at = format!(
            "seed {seed} cycle {cycle} ({}, {mode:?}, killed after {delay:?})",
            backend_name(backend)
        );
        assert!(
            acks.exited.is_none(),
            "{at}: the child exited before the kill: {}",
            acks.exited.unwrap()
        );
        assert!(
            acks.errors.is_empty(),
            "{at}: the child failed a commit: {:?}",
            acks.errors
        );
        if !acks.ready {
            killed_before_ready += 1;
        }
        let db = Pigeonhole::open(&path, options(IoBackend::Pread)).unwrap_or_else(|e| {
            panic!(
                "{at}: reopen failed: {e} (the database is at {})",
                path.display()
            )
        });
        let counts = recovered(&db)
            .unwrap_or_else(|e| panic!("{at}: {e} (the database is at {})", path.display()));
        for th in 0..THREADS {
            let n = counts.get(&(cycle, th)).copied().unwrap_or(0);
            if let Some(last) = acks.last[th as usize] {
                acked_total += last + 1;
                assert!(
                    n > last,
                    "{at}: thread {th} acknowledged batch {last} but only {n} batches survived"
                );
                assert!(
                    n <= last + 2,
                    "{at}: thread {th}: {n} batches, more than the in-flight one past the last acknowledged ({last})"
                );
            } else {
                assert!(
                    n <= 1,
                    "{at}: thread {th} acknowledged nothing but {n} batches appeared"
                );
            }
            expected.insert((cycle, th), n);
        }
        for (&(c, th), &n) in &expected {
            assert_eq!(
                counts.get(&(c, th)).copied().unwrap_or(0),
                n,
                "{at}: cycle {c} thread {th} changed after a later kill"
            );
        }
        drop(counts);
        db.close()
            .unwrap_or_else(|e| panic!("{at}: close failed: {e}"));
    }
    if let Some(d) = dir.take() {
        let _ = std::fs::remove_dir_all(&d);
    }
    let _ = std::fs::remove_dir(&root);
    eprintln!(
        "kill loop: {cycles} cycles in {:?}, {acked_total} acknowledged batches checked, {killed_before_ready} kills before the open finished",
        started.elapsed()
    );
}
