//! Engine hot paths (issue #15, decision D29): point gets on memtable-resident data by value
//! size (the copy threshold) and view-pin strategy (`get_latest` through an `arc-swap`
//! guard versus `get` through a cloned `Arc<View>`), single-threaded and with every core
//! reading; single-shard commit latency per durability level on real files; group-commit
//! throughput with many committers; and write scaling from one to N shards.
//!
//! Run with `cargo bench -p pigeonhole-engine`. The write benchmarks use `PreadVfs` on a
//! temporary directory, so their numbers depend on the disk (D5: non-reference hardware).

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_engine::{
    Durability, Engine, EngineOptions, FamilyOptions, TableInfo, ValueRef, WriteBatch,
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

criterion_group!(benches, gets, commits, scaling);
criterion_main!(benches);
