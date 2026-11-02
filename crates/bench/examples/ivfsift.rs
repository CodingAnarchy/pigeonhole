//! D306 PR 3 (#490 D): the IVF index (`pigeonhole-vector`) on SIFT1M, against D306's target
//! of 95% recall@10 at ≤10 ms per query, single-threaded.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example ivfsift -- SIFT_DIR DB_DIR MODE [N] [TARGET] [DURABILITY]
//! ```
//!
//! `SIFT_DIR` holds the standard files `sift_base.fvecs`, `sift_query.fvecs` and
//! `sift_groundtruth.ivecs` (ftp.irisa.fr/local/texmex/corpus/sift.tar.gz). `MODE` is
//! `bulk` (`Index::bulk_load_f32`) or `insert` (one `insert_f32` per vector: the
//! self-rebalancing path, splits inline). `N` loads only the first `N` base vectors, and then
//! the ground truth is recomputed by brute force over those, so a small run still measures
//! true recall; `all` (the default) loads the whole base against the dataset's ground truth. `TARGET` is the list length (default 1,000, D306 §5).
//! `DURABILITY` is the commits' (`none`, `buffered` (default), `groupsync` or `sync`): an
//! insert is one transaction, so `sync` makes every insert wait for an fsync.
//!
//! Prints JSON lines: the build (seconds, vectors, lists, and in `insert` mode the insert
//! latencies' p50/p99/p999 in µs), then, for each `nprobe` in 4, 8, 16, 32, 64 and 128,
//! recall@10 over the queries and per-query latency p50/p99 (µs), single-threaded, both
//! `run` and `run_async` (the latter on a minimal executor), then a cold row for the
//! smallest `nprobe` meeting 95% recall: a reopen with direct I/O (so neither the block
//! cache nor the page cache holds the lists), then the same queries once, sync and async.
//! Each warm row first runs the queries once untimed.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use pigeonhole::{Durability, Element, Metric, Options, Pigeonhole, VectorSpec};
use pigeonhole_vector::{Index, IndexOptions};

/// The nprobes swept, unless `IVF_NPROBES` lists others (comma-separated).
const NPROBES: [u32; 6] = [4, 8, 16, 32, 64, 128];

fn nprobes() -> Vec<u32> {
    match std::env::var("IVF_NPROBES") {
        Ok(v) => v
            .split(',')
            .map(|n| n.trim().parse().expect("IVF_NPROBES"))
            .collect(),
        Err(_) => NPROBES.to_vec(),
    }
}
const K: usize = 10;

/// Reads a `.fvecs` file: each record is a little-endian `i32` dimension, then that many
/// `f32`s. At most `limit` records.
fn fvecs(path: &Path, limit: usize) -> Vec<Vec<f32>> {
    let mut r = BufReader::new(File::open(path).unwrap_or_else(|e| panic!("{path:?}: {e}")));
    let mut out = Vec::new();
    let mut word = [0u8; 4];
    while out.len() < limit && r.read_exact(&mut word).is_ok() {
        let dim = i32::from_le_bytes(word) as usize;
        let mut buf = vec![0u8; 4 * dim];
        r.read_exact(&mut buf).expect("a truncated .fvecs record");
        out.push(
            buf.as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c))
                .collect(),
        );
    }
    out
}

/// Reads a `.ivecs` file, as [`fvecs`] with `i32`s.
fn ivecs(path: &Path, limit: usize) -> Vec<Vec<u32>> {
    let mut r = BufReader::new(File::open(path).unwrap_or_else(|e| panic!("{path:?}: {e}")));
    let mut out = Vec::new();
    let mut word = [0u8; 4];
    while out.len() < limit && r.read_exact(&mut word).is_ok() {
        let dim = i32::from_le_bytes(word) as usize;
        let mut buf = vec![0u8; 4 * dim];
        r.read_exact(&mut buf).expect("a truncated .ivecs record");
        out.push(
            buf.as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes(*c))
                .collect(),
        );
    }
    out
}

/// The `K` nearest base vectors to `q` by squared L2, ties by index: the ground truth for a
/// partial base.
fn brute(base: &[Vec<f32>], q: &[f32]) -> Vec<u32> {
    let mut all: Vec<(f32, u32)> = base
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v.iter().zip(q).map(|(a, b)| (a - b) * (a - b)).sum();
            (d, i as u32)
        })
        .collect();
    all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    all.into_iter().take(K).map(|(_, i)| i).collect()
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize] as f64
}

fn id(i: usize) -> [u8; 4] {
    (i as u32).to_be_bytes()
}

/// Runs every query at `nprobe`: recall@10 against `truth`, and the per-query latencies (ns).
fn measure(
    index: &Index,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    nprobe: u32,
    asynchronous: bool,
) -> (f64, Vec<u64>) {
    let mut found = 0usize;
    let mut lat = Vec::with_capacity(queries.len());
    for (q, t) in queries.iter().zip(truth) {
        let start = Instant::now();
        let hits = if asynchronous {
            pigeonhole::doc_support::block_on(index.search_f32(q, K).nprobe(nprobe).run_async())
        } else {
            index.search_f32(q, K).nprobe(nprobe).run()
        }
        .expect("search");
        lat.push(start.elapsed().as_nanos() as u64);
        let want = &t[..K.min(t.len())];
        found += hits
            .iter()
            .filter(|h| {
                let i = u32::from_be_bytes(h.id.as_slice().try_into().expect("a 4-byte id"));
                want.contains(&i)
            })
            .count();
    }
    lat.sort_unstable();
    (found as f64 / (queries.len() * K) as f64, lat)
}

fn row(what: &str, nprobe: u32, recall: f64, lat: &[u64]) {
    println!(
        "{{\"row\":\"{what}\",\"nprobe\":{nprobe},\"recall10\":{recall:.4},\
         \"p50_us\":{:.1},\"p99_us\":{:.1}}}",
        percentile(lat, 0.5) / 1e3,
        percentile(lat, 0.99) / 1e3
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = "ivfsift SIFT_DIR DB_DIR bulk|insert [N] [TARGET] [DURABILITY]";
    let sift = PathBuf::from(args.get(1).expect(usage));
    let db_dir = PathBuf::from(args.get(2).expect(usage));
    let mode = args.get(3).expect(usage).as_str();
    // `all` (or none): the whole base, scored against the dataset's ground truth.
    let n: usize = match args.get(4).map(String::as_str) {
        None | Some("all") => usize::MAX,
        Some(s) => s.parse().expect(usage),
    };
    let target: u32 = args.get(5).map_or(1000, |s| s.parse().expect(usage));
    let durability = match args.get(6).map_or("buffered", String::as_str) {
        "none" => Durability::None,
        "buffered" => Durability::Buffered,
        "groupsync" => Durability::GroupSync,
        "sync" => Durability::Sync,
        _ => panic!("{usage}"),
    };

    let base = fvecs(&sift.join("sift_base.fvecs"), n);
    let queries = fvecs(&sift.join("sift_query.fvecs"), usize::MAX);
    let truth = if n == usize::MAX {
        ivecs(&sift.join("sift_groundtruth.ivecs"), usize::MAX)
    } else {
        queries.iter().map(|q| brute(&base, q)).collect()
    };
    let dim = base.first().expect("an empty base").len() as u32;
    eprintln!("{} base, {} queries, dim {dim}", base.len(), queries.len());

    let path = db_dir.join("ivf.phdb");
    // IVF_BLOCK_CACHE_MB: the block cache, so a run can hold the whole index in memory as
    // Faiss does (default: the database's).
    let cache: Option<usize> = std::env::var("IVF_BLOCK_CACHE_MB")
        .ok()
        .map(|v| v.parse::<usize>().expect("IVF_BLOCK_CACHE_MB") << 20);
    let opened = |o: Options| match cache {
        Some(c) => o.block_cache(c),
        None => o,
    };
    let spec = VectorSpec::new(dim, Element::F32, Metric::L2).expect("spec");
    let options = IndexOptions::new().target_list_len(target);
    {
        let db = Pigeonhole::open(&path, opened(Options::default().durability(durability)))
            .expect("open");
        let index = Index::create(&db, "sift", spec, options).expect("create");
        let start = Instant::now();
        let mut inserts = Vec::new();
        match mode {
            "bulk" => {
                index
                    .bulk_load_f32(
                        base.len() as u64,
                        base.iter().enumerate().map(|(i, v)| (id(i), v)),
                    )
                    .expect("bulk load");
            }
            "insert" => {
                inserts.reserve(base.len());
                for (i, v) in base.iter().enumerate() {
                    let t = Instant::now();
                    index.insert_f32(&id(i), v).expect("insert");
                    inserts.push(t.elapsed().as_nanos() as u64);
                }
            }
            _ => panic!("{usage}"),
        }
        // IVF_COMPACT=1: compact after the build, as a served index ends up (one source per
        // list instead of the memtable and several L0 files merged per cell).
        if std::env::var("IVF_COMPACT").is_ok_and(|v| v == "1") {
            db.compact().expect("compact");
        }
        let secs = start.elapsed().as_secs_f64();
        let stats = index.verify().expect("verify");
        inserts.sort_unstable();
        println!(
            "{{\"row\":\"build\",\"mode\":\"{mode}\",\"durability\":\"{durability:?}\",\"secs\":{secs:.1},\"vectors\":{},\
             \"lists\":{},\"longest\":{},\"insert_p50_us\":{:.1},\"insert_p99_us\":{:.1},\
             \"insert_p999_us\":{:.1}}}",
            stats.vectors,
            stats.list_lens.len(),
            stats.list_lens.iter().max().copied().unwrap_or(0),
            percentile(&inserts, 0.5) / 1e3,
            percentile(&inserts, 0.99) / 1e3,
            percentile(&inserts, 0.999) / 1e3,
        );
        let mut first_95 = None;
        let sweep = nprobes();
        for &nprobe in &sweep {
            for asynchronous in [false, true] {
                // Untimed once: the lists' blocks in the cache, as a serving process has them.
                measure(&index, &queries, &truth, nprobe, asynchronous);
                let (recall, lat) = measure(&index, &queries, &truth, nprobe, asynchronous);
                let what = if asynchronous { "warm_async" } else { "warm" };
                row(what, nprobe, recall, &lat);
                if recall >= 0.95 && first_95.is_none() {
                    first_95 = Some(nprobe);
                }
            }
        }
        drop(index);
        db.close().expect("close");
        // Cold: a reopen with direct I/O (no page cache either), then each query once at the
        // smallest nprobe meeting 95% recall.
        let nprobe = first_95.unwrap_or(*sweep.last().expect("nprobes"));
        for asynchronous in [false, true] {
            let db = Pigeonhole::open(
                &path,
                opened(Options::default().durability(durability).direct_io(true)),
            )
            .expect("reopen");
            let index = Index::open(&db, "sift").expect("open index");
            let (recall, lat) = measure(&index, &queries, &truth, nprobe, asynchronous);
            row(
                if asynchronous { "cold_async" } else { "cold" },
                nprobe,
                recall,
                &lat,
            );
            drop(index);
            db.close().expect("close");
        }
    }
}
