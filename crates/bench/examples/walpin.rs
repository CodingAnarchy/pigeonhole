//! The WAL pin bound's default (#527): `wal_pin_bytes` at 2× or 4× the memtable budget,
//! measured on the write path and on the open after a crash.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example walpin -- write  DIR MULT [COMMITS]
//! cargo run --release -p pigeonhole-bench --example walpin -- reopen DIR MULT
//! ```
//!
//! `write` creates `DIR/walpin.phdb` with slots of mixed temperature, the workload of
//! #175's harness (`engine/tests/wal_pin.rs`) at the default memtable budget:
//! - 4 shards, 8 tables (one tablet each, two per shard) of 4 families: 32 slots;
//! - each commit writes a 1 KB value to a slot drawn with weight `1 / (rank + 1)`;
//! - one commit in 10 also writes a slot of another table, most often on another shard (a
//!   cross-shard commit).
//!
//! It runs with `wal_pin_bytes` = `MULT` × the budget, 8 client threads committing
//! `Buffered`, and samples the WAL files' total size every 20 ms: the disk the WAL holds, in
//! whole preallocated segments, not its live records. Then it prints one JSON line and exits
//! **without closing**: the WAL is left as a crash leaves it (the writes are in the page
//! cache, so nothing is lost; only the clean close is skipped). `WALPIN_CLEAN=1` closes
//! instead (a check that the close finishes, #585).
//!
//! `reopen` opens that database (replaying the WAL), reads one cell, prints one JSON line
//! with the open's and the read's times, and closes cleanly.
//!
//! The backend is the default's: io_uring where the kernel offers it, else pread
//! (`PIGEONHOLE_IO=pread` forces pread).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pigeonhole_engine::{
    Durability, Engine, EngineOptions, FamilyId, FamilyOptions, TableId, ValueRef, WriteBatch,
};
use pigeonhole_io::VfsRef;

const SHARDS: usize = 4;
const TABLES: usize = 8;
const FAMILIES: usize = 4;
const VALUE: usize = 1000;
const CLIENTS: u64 = 8;
const BUDGET: u64 = 64 << 20;

fn vfs() -> VfsRef {
    #[cfg(target_os = "linux")]
    if std::env::var("PIGEONHOLE_IO").as_deref() != Ok("pread")
        && let Ok(v) = pigeonhole_io::uring::UringVfs::new()
    {
        return v;
    }
    pigeonhole_io::pread::PreadVfs::new(0)
}

fn options(mult: u64) -> EngineOptions {
    let mut o = EngineOptions::new(vfs());
    o.create_if_missing = true;
    o.shards = SHARDS;
    o.memtable_budget = BUDGET;
    o.wal_pin_bytes = mult * BUDGET;
    // The placement stays as created: two tables per shard.
    o.tablet_changes = false;
    o
}

/// Bytes in every WAL stream file beside the database.
fn wal_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().contains("-wal-"))
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// xorshift64*: a seeded stream per client, so runs are repeatable.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }
}

/// Slot ranks: weights `1 / (rank + 1)` over a seeded shuffle of the slots.
struct Slots {
    order: Vec<usize>,
    weights: Vec<f64>,
    total: f64,
}

impl Slots {
    fn new(n: usize) -> Self {
        let mut rng = Rng(527);
        let mut order: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            order.swap(i, rng.below(i as u64 + 1) as usize);
        }
        let weights: Vec<f64> = (0..n).map(|r| 1.0 / (r as f64 + 1.0)).collect();
        let total = weights.iter().sum();
        Self {
            order,
            weights,
            total,
        }
    }

    fn pick(&self, rng: &mut Rng) -> usize {
        let mut x = rng.below(1 << 30) as f64 / f64::from(1 << 30) * self.total;
        for (r, w) in self.weights.iter().enumerate() {
            if x < *w {
                return self.order[r];
            }
            x -= w;
        }
        self.order[self.order.len() - 1]
    }
}

fn put(batch: &mut WriteBatch, (table, family): (TableId, FamilyId), row: &[u8], v: &[u8]) {
    batch
        .put(table, family, row, b"q", None, ValueRef::Bytes(v))
        .expect("put");
}

fn write(dir: &Path, mult: u64, commits: u64) {
    std::fs::create_dir_all(dir).expect("dir");
    let path = dir.join("walpin.phdb");
    let db = Engine::open(&path, options(mult)).expect("open");
    let fams: Vec<(String, FamilyOptions)> = (0..FAMILIES)
        .map(|f| (format!("f{f}"), FamilyOptions::default()))
        .collect();
    let slots: Vec<(TableId, FamilyId)> = (0..TABLES)
        .flat_map(|t| {
            let info = db.create_table(&format!("t{t}"), &fams).expect("table");
            (0..FAMILIES)
                .map(|f| (info.id, info.family(&format!("f{f}")).expect("family").id))
                .collect::<Vec<_>>()
        })
        .collect();
    let ranks = Slots::new(slots.len());

    let stop = Arc::new(AtomicBool::new(false));
    let max_wal = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (stop, max_wal, dir) = (Arc::clone(&stop), Arc::clone(&max_wal), dir.to_path_buf());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                max_wal.fetch_max(wal_bytes(&dir), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };
    let start = Instant::now();
    std::thread::scope(|s| {
        for c in 0..CLIENTS {
            let (db, slots, ranks) = (&db, &slots, &ranks);
            s.spawn(move || {
                let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (c + 1));
                let v = vec![7u8; VALUE];
                let mut i = c;
                while i < commits {
                    let row = i.to_be_bytes();
                    let mut wb = WriteBatch::new();
                    let a = ranks.pick(&mut rng);
                    put(&mut wb, slots[a], &row, &v);
                    if rng.below(10) == 0 {
                        let b = ranks.pick(&mut rng);
                        if slots[b].0 != slots[a].0 {
                            put(&mut wb, slots[b], &row, &v);
                        }
                    }
                    db.commit(wb, Some(Durability::Buffered)).expect("commit");
                    i += CLIENTS;
                }
            });
        }
    });
    let secs = start.elapsed().as_secs_f64();
    stop.store(true, Ordering::Release);
    sampler.join().expect("sampler");
    let at_crash = wal_bytes(dir);
    let max_wal = max_wal.load(Ordering::Relaxed).max(at_crash);
    let m = db.metrics();
    println!(
        "{{\"mode\":\"write\",\"mult\":{mult},\"budget\":{BUDGET},\"shards\":{SHARDS},\
         \"commits\":{commits},\"secs\":{secs:.3},\"commits_per_sec\":{:.0},\
         \"unpin_passes\":{},\"small_flushes\":{},\"flushes\":{},\"compactions\":{},\
         \"max_wal_bytes\":{max_wal},\"wal_bytes_at_crash\":{at_crash}}}",
        commits as f64 / secs,
        m.unpin.0,
        m.unpin.1,
        m.flushes,
        m.compactions,
    );
    if std::env::var_os("WALPIN_CLEAN").is_some() {
        db.close().expect("close");
        return;
    }
    // A crash, as far as the database can tell: no close, no final flush or checkpoint.
    std::process::exit(0);
}

fn reopen(dir: &Path, mult: u64) {
    let path = dir.join("walpin.phdb");
    let wal = wal_bytes(dir);
    let start = Instant::now();
    let db = Engine::open(&path, options(mult)).expect("reopen");
    let open = start.elapsed();
    let table = db.table("t0").expect("table t0");
    let start = Instant::now();
    {
        // Dropped before the close, which waits for open snapshots.
        let snapshot = db.snapshot().expect("snapshot");
        let _ = db
            .get(
                &snapshot,
                table.id,
                table.family("f0").expect("f0").id,
                &0u64.to_be_bytes(),
                b"q",
            )
            .expect("read");
    }
    let read = start.elapsed();
    eprintln!("walpin: reopened in {open:?}, first read {read:?}; closing");
    db.close().expect("close");
    println!(
        "{{\"mode\":\"reopen\",\"mult\":{mult},\"wal_bytes\":{wal},\"open_ms\":{:.1},\
         \"first_read_us\":{:.0}}}",
        open.as_secs_f64() * 1e3,
        read.as_secs_f64() * 1e6,
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: walpin write DIR MULT [COMMITS] | walpin reopen DIR MULT";
    let (mode, dir, mult) = match args.as_slice() {
        [_, mode, dir, mult, ..] => (
            mode.as_str(),
            PathBuf::from(dir),
            mult.parse::<u64>().expect(usage),
        ),
        _ => panic!("{usage}"),
    };
    match mode {
        "write" => write(
            &dir,
            mult,
            args.get(4)
                .map_or(4_000_000, |c| c.parse().expect("COMMITS")),
        ),
        "reopen" => reopen(&dir, mult),
        _ => panic!("{usage}"),
    }
}
