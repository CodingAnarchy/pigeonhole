//! Group-commit throughput and commit latency per durability level on real files.
//!
//! Each "commit" appends one Batch record of a 100-byte value. A group of `g` commits is one
//! `write()` and, for `GroupSync`, one submitted fdatasync that the group waits on; `Sync`
//! is one blocking fdatasync per commit. Before the criterion groups run, a short sampling
//! loop prints p50 and p99 commit latency per level.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_format::wal::{BatchBuilder, WalRecord};
use pigeonhole_format::{Durability, FamilyId, Kind, StreamId, TableId};
use pigeonhole_io::VfsRef;
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_wal::{Wal, WalOptions, WalStream};

struct Bench {
    wal: WalStream,
    batch: BatchBuilder,
    seqno: u64,
    _dir: TempDir,
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Bench {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("pigeonhole-wal-bench-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let vfs: VfsRef = PreadVfs::new(2);
        let mut opts = WalOptions::default();
        opts.spare_segments = 1;
        let wal =
            WalStream::create(&vfs, &dir.join("bench.phdb"), StreamId(0), [1; 16], opts).unwrap();
        let mut batch = BatchBuilder::new();
        batch
            .push(
                TableId(1),
                FamilyId(1),
                Kind::Put,
                b"com.example/000001",
                b"links:001",
                None,
                &[7u8; 100],
            )
            .unwrap();
        Self {
            wal,
            batch,
            seqno: 0,
            _dir: TempDir(dir),
        }
    }

    /// One group of `g` commits at `level`; returns once every member's level is met.
    fn group(&mut self, g: usize, level: Durability) {
        let mut last = None;
        for _ in 0..g {
            self.seqno += 1;
            let rec = WalRecord::Batch {
                seqno: self.seqno,
                commit_ts: self.seqno,
                batch: self.batch.batch(),
            };
            last = Some(self.wal.append(&rec, level).unwrap());
            if level == Durability::Sync {
                self.wal.sync().unwrap();
            }
        }
        match level {
            Durability::Sync => {}
            Durability::GroupSync => {
                self.wal.submit_sync().unwrap().wait().unwrap();
            }
            _ => {
                self.wal.write().unwrap();
            }
        }
        let t = last.unwrap();
        debug_assert!(self.wal.satisfies(&t));
        // Keep the file at its preallocated size: everything before the current segment is
        // "flushed".
        let written = self.wal.written();
        self.wal.checkpoint(written).unwrap();
        black_box(t);
    }
}

fn percentiles(tag: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let p = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    println!(
        "{tag}: p50 {:?}  p99 {:?}  max {:?}  ({} samples)",
        p(0.5),
        p(0.99),
        samples.last().unwrap(),
        samples.len()
    );
}

fn latency_report() {
    for (name, level, g) in [
        ("buffered g=1", Durability::Buffered, 1),
        ("group_sync g=1", Durability::GroupSync, 1),
        ("group_sync g=16", Durability::GroupSync, 16),
        ("sync g=1", Durability::Sync, 1),
    ] {
        let mut b = Bench::new(&name.replace(' ', "-").replace('=', ""));
        let n = if level == Durability::Buffered {
            20_000
        } else {
            400
        };
        let mut samples = Vec::with_capacity(n);
        for _ in 0..n {
            let start = Instant::now();
            b.group(g, level);
            samples.push(start.elapsed());
        }
        percentiles(&format!("commit latency {name}"), samples);
    }
}

fn bench_commit(c: &mut Criterion) {
    latency_report();
    let mut grp = c.benchmark_group("wal_commit");
    for (name, level, groups) in [
        ("buffered", Durability::Buffered, &[1usize, 16, 128][..]),
        ("group_sync", Durability::GroupSync, &[1, 16, 128][..]),
        ("sync", Durability::Sync, &[1][..]),
    ] {
        for &g in groups {
            let mut b = Bench::new(&format!("{name}-{g}"));
            grp.throughput(Throughput::Elements(g as u64));
            if level != Durability::Buffered {
                grp.sample_size(20).measurement_time(Duration::from_secs(5));
            }
            grp.bench_with_input(BenchmarkId::new(name, g), &g, |bench, &g| {
                bench.iter(|| b.group(g, level));
            });
        }
    }
    grp.finish();
}

criterion_group!(benches, bench_commit);
criterion_main!(benches);
