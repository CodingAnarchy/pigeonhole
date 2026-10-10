//! Cold point gets (#87; spec: with index and filters pinned, a cold get is one data-block
//! I/O, about 20 to 80 µs on NVMe): the latency of gets of rows the block cache does not
//! hold, and how many block reads each one cost.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example coldget -- load DIR MIB
//! cargo run --release -p pigeonhole-bench --example coldget -- get DIR
//! ```
//!
//! `load` writes about MIB mebibytes of rows (one 1024-byte cell each) into `DIR/cold.phdb`
//! in a random key order, so background compaction builds the leveled tree a random-insert
//! workload has (no final full compaction), then waits for compaction to go quiet.
//!
//! `get` reopens it with a 64 MiB block cache and runs, on `COLDGET_THREADS` threads
//! (default 1), `COLDGET_WARMUP` warm-up gets (default 100,000), then `COLDGET_GETS` measured
//! gets (default 1,000,000) of rows chosen uniformly from the whole table, then as many gets
//! of absent rows. An absent row sorts between two present ones, so only a filter rules it
//! out. It prints one JSON line per phase: latency percentiles, gets per second,
//! block-cache misses per get (each miss is one block read from the file), and, when
//! `COLDGET_BLOCKDEV` names a Linux block device (`nvme0n1`, `md0`), the device's completed
//! reads per get from `/sys/block/DEV/stat`.
//!
//! It measures nothing about the page cache: open with direct I/O (`PIGEONHOLE_DIRECT=1`),
//! or cap the process's memory (a cgroup) and empty the page cache before each `get`, as
//! `baselines/phase3-io/cold-get.sh` does. The backend comes from `PIGEONHOLE_IO` (`pread`,
//! `uring`), as for the test suites; the JSON line names the environment it ran with.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};
use pigeonhole_bench::Histogram;

const VALUE: usize = 1024;
/// Rows per batch while loading.
const BATCH: u64 = 256;
const CACHE: usize = 64 << 20;

fn key(i: u64) -> Vec<u8> {
    format!("row{i:012}").into_bytes()
}

/// A row between `key(i)` and `key(i + 1)`: absent, and inside the key range of whichever
/// SSTs hold its neighbours, so fences cannot rule it out and only a filter can.
fn absent(i: u64) -> Vec<u8> {
    format!("row{i:012}x").into_bytes()
}

/// A permutation of `0..rows`: a bijection on `bits`-bit integers (multiply by an odd
/// constant, then an xorshift), cycle-walked until it lands below `rows`.
fn permute(i: u64, bits: u32, rows: u64) -> u64 {
    let mask = (1u64 << bits) - 1;
    let mut x = i;
    loop {
        x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15) & mask;
        x ^= x >> (bits / 2).max(1);
        if x < rows {
            return x;
        }
    }
}

/// SplitMix64, so each thread's choice of rows is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % n
    }
}

fn open(dir: &Path) -> Pigeonhole {
    Pigeonhole::open(dir.join("cold.phdb"), Options::default().block_cache(CACHE)).unwrap()
}

fn rows_file(dir: &Path) -> std::path::PathBuf {
    dir.join("coldget.rows")
}

fn load(dir: &Path, mib: u64) {
    std::fs::create_dir_all(dir).unwrap();
    let db = open(dir);
    db.set_default_durability(Durability::Buffered);
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let rows = (mib << 20) / VALUE as u64;
    let bits = 64 - rows.max(2).saturating_sub(1).leading_zeros();
    let start = Instant::now();
    // Each thread writes the rows at its positions in the permutation, in single-shard batches
    // (`Table::shard_of`): a batch of random rows would span every shard and commit as a
    // cross-shard transaction, several times slower to load.
    let loaders = std::thread::available_parallelism()
        .map_or(4, |n| n.get() as u64)
        .min(16);
    std::thread::scope(|s| {
        for l in 0..loaders {
            let (db, t) = (&db, &t);
            s.spawn(move || {
                let mut value = vec![0u8; VALUE];
                let mut pending: HashMap<usize, (pigeonhole::WriteBatch, u64)> = HashMap::new();
                let mut j = l;
                while j < rows {
                    let r = permute(j, bits, rows);
                    let k = key(r);
                    // Varied bytes, so compression does not shrink the table.
                    for (n, b) in value.iter_mut().enumerate() {
                        *b = (r as usize).wrapping_mul(31).wrapping_add(n * 7) as u8
                            ^ (n >> 3) as u8;
                    }
                    let shard = t.shard_of(&k).unwrap();
                    let (batch, n) = pending
                        .entry(shard)
                        .or_insert_with(|| (db.write_batch(), 0));
                    batch.put(t, &k, "f", b"v", &value);
                    *n += 1;
                    if *n == BATCH {
                        let (full, _) = pending.remove(&shard).unwrap();
                        full.commit().unwrap();
                    }
                    j += loaders;
                }
                for (_, (batch, _)) in pending {
                    batch.commit().unwrap();
                }
            });
        }
    });
    drop(t);
    db.flush().unwrap();
    // Let background compaction finish what the load started, so `get` measures a quiet tree:
    // done once `COLDGET_QUIET_SECS` (default 60) pass with no compaction finishing.
    let quiet = Duration::from_secs(env_or("COLDGET_QUIET_SECS", 60));
    let mut done = db.engine_metrics().compactions;
    loop {
        std::thread::sleep(quiet);
        let now = db.engine_metrics().compactions;
        if now == done {
            break;
        }
        done = now;
    }
    db.close().unwrap();
    std::fs::write(rows_file(dir), rows.to_string()).unwrap();
    eprintln!(
        "loaded {rows} rows in {:.0} s ({done} compactions)",
        start.elapsed().as_secs_f64()
    );
}

/// The completed reads of a Linux block device (`/sys/block/DEV/stat`, field 1).
fn device_reads(dev: Option<&str>) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/sys/block/{}/stat", dev?)).ok()?;
    stat.split_whitespace().next()?.parse().ok()
}

fn env_or(k: &str, default: u64) -> u64 {
    std::env::var(k)
        .ok()
        .map_or(default, |v| v.parse().unwrap())
}

/// `gets` gets on `threads` threads, each of `row(rng.below(rows))`: every get's latency,
/// and how many found their row.
fn run(
    t: &Table,
    threads: u64,
    gets: u64,
    rows: u64,
    seed: u64,
    row: fn(u64) -> Vec<u8>,
) -> (Histogram, u64) {
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|n| {
                let t = t.clone();
                s.spawn(move || {
                    let mut rng = Rng(seed ^ (n + 1).wrapping_mul(0xA24B_AED4_963E_E407));
                    let (mut hist, mut found) = (Histogram::new(), 0);
                    for _ in 0..gets / threads {
                        let k = row(rng.below(rows));
                        let start = Instant::now();
                        let row = t.row(&k).family("f").read().unwrap();
                        hist.record(start.elapsed());
                        found += u64::from(std::hint::black_box(row).is_some());
                    }
                    (hist, found)
                })
            })
            .collect();
        let (mut all, mut found) = (Histogram::new(), 0);
        for h in handles {
            let (hist, n) = h.join().unwrap();
            all.merge(&hist);
            found += n;
        }
        (all, found)
    })
}

fn get(dir: &Path) {
    let rows: u64 = std::fs::read_to_string(rows_file(dir))
        .expect("run `coldget load` first")
        .trim()
        .parse()
        .unwrap();
    let threads = env_or("COLDGET_THREADS", 1).max(1);
    let warmup = env_or("COLDGET_WARMUP", 100_000);
    let gets = env_or("COLDGET_GETS", 1_000_000);
    let dev = std::env::var("COLDGET_BLOCKDEV").ok();
    let env = |k: &str| std::env::var(k).unwrap_or_default();

    let db = open(dir);
    let t = db.table("t").unwrap().open().unwrap();
    run(&t, threads, warmup, rows, 1, key);
    for (phase, row, seed) in [
        ("present", key as fn(u64) -> Vec<u8>, 2),
        ("absent", absent, 3),
    ] {
        let (_, misses_before) = db.engine_metrics().block_cache;
        let device_before = device_reads(dev.as_deref());
        let start = Instant::now();
        let (hist, found) = run(&t, threads, gets, rows, seed, row);
        let secs = start.elapsed().as_secs_f64();
        let (_, misses_after) = db.engine_metrics().block_cache;
        let n = hist.count() as f64;
        let device = match (device_before, device_reads(dev.as_deref())) {
            (Some(b), Some(a)) => format!("{:.3}", (a - b) as f64 / n),
            _ => "null".to_owned(),
        };
        let us = |q: f64| hist.percentile(q).as_secs_f64() * 1e6;
        println!(
            "{{\"phase\":\"{phase}\",\"rows\":{rows},\"threads\":{threads},\"gets\":{},\
             \"p50_us\":{:.1},\"p99_us\":{:.1},\"p999_us\":{:.1},\"max_us\":{:.1},\
             \"found\":{:.4},\"gets_per_sec\":{:.0},\"cache_misses_per_get\":{:.3},\
             \"device_reads_per_get\":{device},\"io\":\"{}\",\"direct\":\"{}\"}}",
            hist.count(),
            us(0.5),
            us(0.99),
            us(0.999),
            hist.max().as_secs_f64() * 1e6,
            found as f64 / n,
            n / secs,
            (misses_after - misses_before) as f64 / n,
            env("PIGEONHOLE_IO"),
            env("PIGEONHOLE_DIRECT"),
        );
    }
    drop(t);
    db.close().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("load") if args.len() == 4 => load(Path::new(&args[2]), args[3].parse().unwrap()),
        Some("get") if args.len() == 3 => get(Path::new(&args[2])),
        _ => {
            eprintln!("usage: coldget load DIR MIB | coldget get DIR");
            std::process::exit(2);
        }
    }
}
