//! Engine hot paths (issue #15, decision D29): point gets on memtable-resident data by value
//! size (the copy threshold) and view-pin strategy (`get_latest` through an `arc-swap`
//! guard versus `get` through a cloned `Arc<View>`), single-threaded and with every core
//! reading; single-shard commit latency per durability level on real files; group-commit
//! throughput with many committers; and write scaling from one to N shards.
//!
//! Issue #38 adds write scaling on one table whose tablets the balancer spreads over the
//! shards.
//!
//! Milestone B adds the LSM paths (issue #37): sustained writes with flushes and
//! compactions running, point gets on flushed data with a hot and a cold block cache, and
//! scan throughput over SSTs.
//!
//! Run with `cargo bench -p pigeonhole-engine`. The write benchmarks use `PreadVfs` on a
//! temporary directory, so their numbers depend on the disk (D5: non-reference hardware).

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_engine::{
    Durability, Engine, EngineOptions, FamilyOptions, ScanSpec, TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::sim::SimVfs;

fn options(vfs: VfsRef, shards: usize) -> EngineOptions {
    let mut o = EngineOptions::new(vfs);
    o.create_if_missing = true;
    o.shards = shards;
    o.pin_threads = false;
    o.memtable_budget = 256 << 20;
    o.memtable_freeze_bytes = 32 << 20;
    o.reader_slots = 8;
    o.wal.segment_size = 16 << 20;
    o
}

fn temp_db(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pigeonhole-engine-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.phdb"));
    for p in [path.clone(), path.with_extension("phdb-wal-0")] {
        let _ = std::fs::remove_file(p);
    }
    path
}

fn put(wb: &mut WriteBatch, t: &TableInfo, row: &[u8], value: &[u8]) {
    let f = t.families[0].id;
    wb.put(t.id, f, row, b"q", None, ValueRef::Bytes(value))
        .unwrap();
}

/// Point gets on memtable-resident data (the in-memory `SimVfs`, so no disk is involved).
fn gets(c: &mut Criterion) {
    let vfs: VfsRef = SimVfs::new(1);
    let db = Engine::open("/bench/data.phdb".as_ref(), options(vfs, 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let f = t.families[0].id;
    let sizes = [16usize, 64, 128, 256, 512, 4096];
    for size in sizes {
        let mut wb = WriteBatch::new();
        put(
            &mut wb,
            &t,
            format!("row-{size}").as_bytes(),
            &vec![0xAB; size],
        );
        db.commit(wb, Some(Durability::None)).unwrap();
    }
    // A thousand more rows so the skiplist has some depth.
    let mut wb = WriteBatch::new();
    for i in 0..1000u32 {
        put(&mut wb, &t, format!("filler-{i:05}").as_bytes(), b"x");
    }
    db.commit(wb, Some(Durability::None)).unwrap();

    let mut group = c.benchmark_group("get");
    for size in sizes {
        let row = format!("row-{size}");
        group.bench_with_input(
            BenchmarkId::new("get_latest (arc-swap guard)", size),
            &row,
            |b, row| {
                b.iter(|| {
                    black_box(
                        db.get_latest(t.id, f, row.as_bytes(), b"q")
                            .unwrap()
                            .unwrap(),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("get (snapshot, Arc<View> clone)", size),
            &row,
            |b, row| {
                b.iter(|| {
                    let snap = db.snapshot().unwrap();
                    black_box(
                        db.get(&snap, t.id, f, row.as_bytes(), b"q")
                            .unwrap()
                            .unwrap(),
                    )
                })
            },
        );
    }
    group.bench_function("get_latest miss", |b| {
        b.iter(|| black_box(db.get_latest(t.id, f, b"absent", b"q").unwrap()))
    });
    group.finish();

    // Every core reading the same small and large cells at once.
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut group = c.benchmark_group("get-all-cores");
    group.sample_size(10);
    for (name, size) in [("small", 64usize), ("large", 512)] {
        for strategy in ["get_latest", "get"] {
            group.bench_function(BenchmarkId::new(strategy, name), |b| {
                b.iter_custom(|iters| {
                    let per = iters / cores as u64 + 1;
                    let start = Instant::now();
                    std::thread::scope(|s| {
                        for _ in 0..cores {
                            let db = &db;
                            let t = &t;
                            s.spawn(move || {
                                let row = format!("row-{size}");
                                for _ in 0..per {
                                    if strategy == "get_latest" {
                                        black_box(
                                            db.get_latest(t.id, f, row.as_bytes(), b"q").unwrap(),
                                        );
                                    } else {
                                        let snap = db.snapshot().unwrap();
                                        black_box(
                                            db.get(&snap, t.id, f, row.as_bytes(), b"q").unwrap(),
                                        );
                                    }
                                }
                            });
                        }
                    });
                    start.elapsed() / cores as u32
                })
            });
        }
    }
    group.finish();
    db.close().unwrap();
}

/// Single-shard commit latency per durability level, on real files.
fn commits(c: &mut Criterion) {
    let vfs: VfsRef = PreadVfs::new(2);
    let path = temp_db("commit");
    let db = Engine::open(&path, options(vfs, 1)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut group = c.benchmark_group("commit-1-shard");
    group.sample_size(20);
    let mut i = 0u64;
    for d in [
        Durability::None,
        Durability::Buffered,
        Durability::GroupSync,
        Durability::Sync,
    ] {
        group.bench_function(format!("{d:?}"), |b| {
            // Without flushes the arena is finite: cap the commits per sample and scale.
            b.iter_custom(|iters| {
                let n = iters.min(5_000);
                let start = Instant::now();
                for _ in 0..n {
                    i += 1;
                    let mut wb = WriteBatch::new();
                    put(
                        &mut wb,
                        &t,
                        &i.to_be_bytes(),
                        b"value-of-32-bytes-padding-......",
                    );
                    black_box(db.commit(wb, Some(d)).unwrap());
                }
                start.elapsed().mul_f64(iters as f64 / n as f64)
            })
        });
    }
    group.finish();

    // Group commit: many threads committing to one shard with GroupSync.
    let mut group = c.benchmark_group("group-commit-1-shard");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("GroupSync committers", threads), |b| {
            b.iter_custom(|iters| {
                let per = (iters / threads as u64 + 1).min(2_000);
                let start = Instant::now();
                std::thread::scope(|s| {
                    for k in 0..threads {
                        let db = &db;
                        let t = &t;
                        s.spawn(move || {
                            for j in 0..per {
                                let mut wb = WriteBatch::new();
                                put(&mut wb, t, format!("g{k}-{j}").as_bytes(), b"v");
                                db.commit(wb, Some(Durability::GroupSync)).unwrap();
                            }
                        });
                    }
                });
                // Per-commit latency as seen by one committer, scaled to criterion's count.
                (start.elapsed() / threads as u32)
                    .mul_f64(iters as f64 / (per * threads as u64) as f64)
            })
        });
    }
    group.finish();
    db.close().unwrap();
}

/// Write throughput from one to N shards: N tables (one tablet each, so one per shard) with
/// one committer thread per table, Buffered durability (no fsync in the loop).
fn scaling(c: &mut Criterion) {
    let max = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .min(8);
    let mut group = c.benchmark_group("scaling-buffered");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));
    let mut shards = 1;
    while shards <= max {
        let vfs: VfsRef = PreadVfs::new(2);
        let path = temp_db(&format!("scale-{shards}"));
        let db = Engine::open(&path, options(vfs, shards)).unwrap();
        // One table per shard: tablets are assigned round-robin by id.
        let mut tables: Vec<Option<Arc<TableInfo>>> = vec![None; shards];
        let mut n = 0;
        while tables.iter().any(Option::is_none) {
            let t = db
                .create_table(&format!("t{n}"), &[("f".into(), FamilyOptions::default())])
                .unwrap();
            n += 1;
            let snap = db.snapshot().unwrap();
            let shard = snap.view().tablets().route(t.id, b"x").unwrap().1.0 as usize;
            if tables[shard].is_none() {
                tables[shard] = Some(t);
            }
        }
        let tables: Vec<Arc<TableInfo>> = tables.into_iter().map(Option::unwrap).collect();
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("commits/s per thread", shards), |b| {
            b.iter_custom(|iters| {
                let per = (iters / shards as u64 + 1).min(5_000);
                let start = Instant::now();
                std::thread::scope(|s| {
                    for t in &tables {
                        let db = &db;
                        s.spawn(move || {
                            for j in 0..per {
                                let mut wb = WriteBatch::new();
                                put(
                                    &mut wb,
                                    t,
                                    &j.to_be_bytes(),
                                    b"value-of-32-bytes-padding-......",
                                );
                                db.commit(wb, Some(Durability::Buffered)).unwrap();
                            }
                        });
                    }
                });
                (start.elapsed() / shards as u32)
                    .mul_f64(iters as f64 / (per * shards as u64) as f64)
            })
        });
        db.close().unwrap();
        shards *= 2;
    }
    group.finish();
}

/// Write scaling on one table (issue #38): N committer threads writing hashed rows of a
/// single table. The balancer splits the table's one tablet over the shards and moves the
/// pieces during a warmup; the measurement starts once every shard owns part of the table.
fn scaling_one_table(c: &mut Criterion) {
    let max = std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .min(8);
    let mut group = c.benchmark_group("scaling-one-table-buffered");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));
    let mut shards = 1;
    while shards <= max {
        let vfs: VfsRef = PreadVfs::new(2);
        let path = temp_db(&format!("scale-one-{shards}"));
        let mut o = options(vfs, shards);
        o.tablet_changes = true;
        o.balance_interval_nanos = 20_000_000;
        let db = Engine::open(&path, o).unwrap();
        let t = db
            .create_table("t", &[("f".into(), FamilyOptions::default())])
            .unwrap();
        let row = |i: u64| format!("{:016x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes();
        // Warm up until the table's tablets cover every shard (or 10 s pass).
        let started = Instant::now();
        let mut next = 0u64;
        loop {
            let owners: std::collections::BTreeSet<u16> = db
                .snapshot()
                .unwrap()
                .view()
                .tablets()
                .ranges(t.id)
                .iter()
                .map(|r| r.1)
                .collect();
            if owners.len() >= shards || started.elapsed() > Duration::from_secs(10) {
                eprintln!(
                    "{shards} shards: table spread over {} shards after {:?}",
                    owners.len(),
                    started.elapsed()
                );
                break;
            }
            std::thread::scope(|s| {
                for k in 0..shards as u64 {
                    let (db, t) = (&db, &t);
                    s.spawn(move || {
                        for j in 0..2_000u64 {
                            let mut wb = WriteBatch::new();
                            put(&mut wb, t, &row(next + k * 2_000 + j), b"warmup");
                            db.commit(wb, Some(Durability::Buffered)).unwrap();
                        }
                    });
                }
            });
            next += shards as u64 * 2_000;
        }
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("commits/s per thread", shards), |b| {
            b.iter_custom(|iters| {
                let per = (iters / shards as u64 + 1).min(5_000);
                let base = next;
                next += per * shards as u64;
                let start = Instant::now();
                std::thread::scope(|s| {
                    for k in 0..shards as u64 {
                        let (db, t) = (&db, &t);
                        s.spawn(move || {
                            for j in 0..per {
                                let mut wb = WriteBatch::new();
                                put(
                                    &mut wb,
                                    t,
                                    &row(base + k * per + j),
                                    b"value-of-32-bytes-padding-......",
                                );
                                db.commit(wb, Some(Durability::Buffered)).unwrap();
                            }
                        });
                    }
                });
                (start.elapsed() / shards as u32)
                    .mul_f64(iters as f64 / (per * shards as u64) as f64)
            })
        });
        db.close().unwrap();
        shards *= 2;
    }
    group.finish();
}

/// Options for the LSM benchmarks: small memtables so flushes and compactions run during
/// the measurement, and a block cache of `cache_bytes`.
fn lsm_options(vfs: VfsRef, cache_bytes: usize) -> EngineOptions {
    let mut o = options(vfs, 1);
    o.memtable_budget = 64 << 20;
    o.memtable_freeze_bytes = 8 << 20;
    o.block_cache_bytes = cache_bytes;
    o.compaction.l0_trigger = 4;
    o
}

/// Sustained write throughput with flushes and compactions running (1 KiB values, one
/// shard, `Buffered`): cells per second, and the commit latency percentiles the engine
/// measured are printed after the run.
fn sustained_writes(c: &mut Criterion) {
    let vfs: VfsRef = PreadVfs::new(2);
    let path = temp_db("sustained");
    let db = Engine::open(&path, lsm_options(vfs, 64 << 20)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let value = vec![7u8; 1024];
    let mut group = c.benchmark_group("sustained-write-1-shard");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));
    let mut i = 0u64;
    group.bench_function("Buffered 1KiB cells/s", |b| {
        b.iter(|| {
            i += 1;
            let mut wb = WriteBatch::new();
            // Random-ish keys so compactions rewrite overlapping ranges.
            let key = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).to_be_bytes();
            put(&mut wb, &t, &key, &value);
            black_box(db.commit(wb, Some(Durability::Buffered)).unwrap());
        })
    });
    group.finish();
    let m = db.metrics();
    let d = Durability::Buffered as usize;
    eprintln!(
        "sustained-write: {} commits, {} flushes, {} compactions, {} stalls; Buffered commit latency p50 {} us, p99 {} us, p99.9 {} us",
        m.commits[d],
        m.flushes,
        m.compactions,
        m.stalls.0,
        m.commit_latency_nanos[d][0] / 1_000,
        m.commit_latency_nanos[d][1] / 1_000,
        m.commit_latency_nanos[d][2] / 1_000
    );
    db.close().unwrap();
}

/// Loads `rows` rows of `value_len`-byte values, flushes and compacts them into SSTs.
fn load_flushed(
    path: &std::path::Path,
    cache_bytes: usize,
    rows: u64,
    value_len: usize,
) -> (Arc<Engine>, Arc<TableInfo>) {
    let vfs: VfsRef = PreadVfs::new(2);
    let db = Engine::open(path, lsm_options(vfs, cache_bytes)).unwrap();
    let t = db
        .create_table("t", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let value = vec![3u8; value_len];
    for i in 0..rows {
        let mut wb = WriteBatch::new();
        put(&mut wb, &t, format!("row{i:08}").as_bytes(), &value);
        db.commit(wb, Some(Durability::None)).unwrap();
    }
    db.flush().unwrap();
    db.compact(None).unwrap();
    (db, t)
}

/// Point gets on flushed data: the same 64 rows again and again (blocks cached), and
/// rows spread over the whole table with a cache far smaller than the data (reads hit the
/// file through the pager).
fn flushed_gets(c: &mut Criterion) {
    const ROWS: u64 = 200_000;
    let mut group = c.benchmark_group("get-flushed");
    group.sample_size(20);
    for (name, cache) in [("hot", 256usize << 20), ("cold", 256 << 10)] {
        let path = temp_db(&format!("get-{name}"));
        let (db, t) = load_flushed(&path, cache, ROWS, 256);
        let f = t.families[0].id;
        let snap = db.snapshot().unwrap();
        let mut i = 0u64;
        group.bench_function(name, |b| {
            b.iter(|| {
                i = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let n = if name == "hot" { i % 64 } else { i % ROWS };
                let row = format!("row{n:08}");
                black_box(db.get(&snap, t.id, f, row.as_bytes(), b"q").unwrap());
            })
        });
        drop(snap);
        db.close().unwrap();
    }
    group.finish();
}

/// Full-table scan over SSTs: bytes of cell values per second.
fn flushed_scans(c: &mut Criterion) {
    const ROWS: u64 = 100_000;
    const VALUE: usize = 512;
    let path = temp_db("scan");
    let (db, t) = load_flushed(&path, 256 << 20, ROWS, VALUE);
    let mut group = c.benchmark_group("scan-flushed");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(ROWS * VALUE as u64));
    group.bench_function("full table 512B values", |b| {
        b.iter(|| {
            let snap = db.snapshot().unwrap();
            let mut cur = db
                .scan(
                    &snap,
                    t.id,
                    ScanSpec::new(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                )
                .unwrap();
            let mut bytes = 0usize;
            while cur.next_row().unwrap() {
                while let Some(cell) = cur.next_cell().unwrap() {
                    bytes += cell.stored.len();
                }
            }
            black_box(bytes)
        })
    });
    group.finish();
    db.close().unwrap();
}

criterion_group!(
    benches,
    gets,
    commits,
    scaling,
    scaling_one_table,
    sustained_writes,
    flushed_gets,
    flushed_scans
);
criterion_main!(benches);
