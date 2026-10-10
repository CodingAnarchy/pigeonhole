//! A cold full-table scan, for the scan readahead (#402 PR 5) and the I/O backends: how fast
//! a forward scan reads a table the block cache and the page cache do not hold.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example coldscan -- load DIR MIB
//! cargo run --release -p pigeonhole-bench --example coldscan -- scan DIR
//! cargo run --release -p pigeonhole-bench --example coldscan -- warm DIR
//! ```
//!
//! `load` writes about MIB mebibytes of rows (one 4000-byte cell each, under ordered row
//! keys) into `DIR/cold.phdb`, then flushes and compacts, so the table is one sorted run of
//! SSTs. `scan` reopens it with a 64 MiB block cache and scans the whole table once on this
//! thread, printing one JSON line: rows, bytes, seconds, MiB/s, and what the readahead did
//! (`pigeonhole_sst::counting_readahead`).
//!
//! `warm` measures the other end, the ordered scan from cache (#29; spec: > 1 GB/s decoded per
//! core): it reopens with a block cache larger than the table, fills it with one untimed
//! pass, then times a second pass on this thread and prints its value bytes per second and
//! its block-cache hits and misses (so the share served from the cache is visible).
//!
//! `scan` measures nothing about the page cache: the caller empties it before each `scan` (on
//! Linux, `sync; echo 3 > /proc/sys/vm/drop_caches` as root), or opens with direct I/O
//! (`PIGEONHOLE_DIRECT=1`). The backend comes from `PIGEONHOLE_IO` (`pread`, `uring`) and the
//! scan readahead from `PIGEONHOLE_READAHEAD` (`N[,merge]`, #431), as for the test suites;
//! the JSON line names the environment it ran with.

use std::path::Path;
use std::time::Instant;

use pigeonhole::{Durability, Family, Options, Pigeonhole, Table};

const VALUE: usize = 4000;
/// Rows per batch while loading.
const BATCH: u64 = 256;

fn key(i: u64) -> Vec<u8> {
    format!("row{i:012}").into_bytes()
}

fn open(dir: &Path, cache: usize) -> Pigeonhole {
    Pigeonhole::open(dir.join("cold.phdb"), Options::default().block_cache(cache)).unwrap()
}

/// One forward scan of the whole table on this thread: rows, and value bytes decoded.
fn pass(t: &Table) -> (u64, u64) {
    let (mut rows, mut bytes) = (0u64, 0u64);
    for row in t.scan_prefix(b"").iter().unwrap() {
        let row = row.unwrap();
        rows += 1;
        bytes += (0..row.len())
            .filter_map(|i| row.entry(i))
            .map(|(_, _, c)| c.value().len() as u64)
            .sum::<u64>();
    }
    (rows, bytes)
}

fn load(dir: &Path, mib: u64) {
    std::fs::create_dir_all(dir).unwrap();
    let db = open(dir, 64 << 20);
    db.set_default_durability(Durability::Buffered);
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let rows = (mib << 20) / VALUE as u64;
    let mut value = vec![0u8; VALUE];
    let mut i = 0;
    while i < rows {
        let mut batch = db.write_batch();
        for j in i..(i + BATCH).min(rows) {
            // Varied bytes, so compression does not shrink the table.
            for (n, b) in value.iter_mut().enumerate() {
                *b = (j as usize).wrapping_mul(31).wrapping_add(n * 7) as u8 ^ (n >> 3) as u8;
            }
            batch.put(&t, &key(j), "f", b"v", &value);
        }
        batch.commit().unwrap();
        i += BATCH;
    }
    drop(t);
    db.flush().unwrap();
    db.compact().unwrap();
    db.close().unwrap();
    eprintln!("loaded {rows} rows");
}

fn scan(dir: &Path) {
    let db = open(dir, 64 << 20);
    let t = db.table("t").unwrap().open().unwrap();
    let start = Instant::now();
    let ((rows, bytes), ahead) = pigeonhole_sst::counting_readahead(|| pass(&t));
    let secs = start.elapsed().as_secs_f64();
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    println!(
        "{{\"rows\":{rows},\"bytes\":{bytes},\"secs\":{secs:.4},\"mib_per_sec\":{:.1},\
         \"readahead_issued\":{},\"readahead_used\":{},\"readahead_waited\":{},\
         \"io\":\"{}\",\"direct\":\"{}\",\"readahead\":\"{}\"}}",
        bytes as f64 / f64::from(1 << 20) / secs,
        ahead.issued,
        ahead.used,
        ahead.waited,
        env("PIGEONHOLE_IO"),
        env("PIGEONHOLE_DIRECT"),
        env("PIGEONHOLE_READAHEAD"),
    );
    drop(t);
    db.close().unwrap();
}

/// The ordered scan from cache (#29; spec: > 1 GB/s decoded per core): a block cache larger
/// than the table, one untimed pass to fill it, then a timed pass on this thread; its
/// block-cache hits and misses are printed, so the share served from the cache is visible.
fn warm(dir: &Path) {
    let file = std::fs::metadata(dir.join("cold.phdb")).unwrap().len();
    let cache = usize::try_from(file + file / 4 + (64 << 20)).unwrap();
    let db = open(dir, cache);
    let t = db.table("t").unwrap().open().unwrap();
    pass(&t);
    let (hits_before, misses_before) = db.engine_metrics().block_cache;
    let start = Instant::now();
    let (rows, bytes) = pass(&t);
    let secs = start.elapsed().as_secs_f64();
    let (hits_after, misses_after) = db.engine_metrics().block_cache;
    println!(
        "{{\"mode\":\"warm\",\"rows\":{rows},\"bytes\":{bytes},\"secs\":{secs:.4},\
         \"mib_per_sec\":{:.1},\"gb_per_sec\":{:.3},\"cache_mib\":{},\"cache_hits\":{},\"cache_misses\":{},\
         \"io\":\"{}\"}}",
        bytes as f64 / f64::from(1 << 20) / secs,
        bytes as f64 / 1e9 / secs,
        cache >> 20,
        hits_after - hits_before,
        misses_after - misses_before,
        std::env::var("PIGEONHOLE_IO").unwrap_or_default(),
    );
    drop(t);
    db.close().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("load") if args.len() == 4 => load(Path::new(&args[2]), args[3].parse().unwrap()),
        Some("scan") if args.len() == 3 => scan(Path::new(&args[2])),
        Some("warm") if args.len() == 3 => warm(Path::new(&args[2])),
        _ => {
            eprintln!("usage: coldscan load DIR MIB | coldscan scan DIR | coldscan warm DIR");
            std::process::exit(2);
        }
    }
}
