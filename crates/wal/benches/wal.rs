//! Group-commit throughput and commit latency per durability level on real files.
//!
//! Each "commit" appends one Batch record of a 100-byte value. A group of `g` commits is one
//! `write()` and, for `GroupSync`, one submitted fdatasync; `Sync` is one blocking fdatasync
//! per commit. `group_sync` waits for each group's sync before building the next (one group
//! in flight); `group_sync_pipelined` keeps `DEPTH` groups in flight, as a shard does.
//!
//! Segments are small (4 MiB) and a warm-up pass cycles through every slot with checkpoints,
//! so the measured path runs on recycled, fully written blocks. Before the criterion groups
//! run, a short sampling loop prints p50 and p99 commit latency per level.

use std::collections::VecDeque;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_format::wal::{BatchBuilder, WalRecord};
use pigeonhole_format::{Durability, FamilyId, Kind, Lsn, StreamId, TableId};
use pigeonhole_io::pread::PreadVfs;
use pigeonhole_io::{Completion, VfsRef};
use pigeonhole_wal::{Wal, WalOptions, WalStream};

/// Groups kept in flight by the pipelined benchmark.
const DEPTH: usize = 4;
const SEGMENT: u64 = 4 << 20;

struct Bench {
    wal: WalStream,
    batch: BatchBuilder,
    seqno: u64,
    in_flight: VecDeque<Completion<Lsn>>,
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
        opts.segment_size = SEGMENT;
        opts.spare_segments = 2;
        let wal =
            WalStream::create(&vfs, &dir.join("bench.phdb"), StreamId(0), [1; 16], opts).unwrap();
        wal.spares().prepare(opts.spare_segments).unwrap();
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
        let mut b = Self {
            wal,
            batch,
            seqno: 0,
            in_flight: VecDeque::new(),
            _dir: TempDir(dir),
        };
        // Warm up: cycle through every slot so the measured segments are recycled ones.
        let first = b.wal.written().epoch();
        while b.wal.written().epoch() < first + 2 * (opts.spare_segments + 1) {
            b.group(64, Durability::Buffered);
        }
        b.wal.sync().unwrap();
        b
    }

    fn append_one(&mut self, level: Durability) -> pigeonhole_wal::CommitTicket {
        self.seqno += 1;
        let rec = WalRecord::Batch {
            seqno: self.seqno,
            commit_ts: self.seqno,
            batch: self.batch.batch(),
        };
        self.wal.append(&rec, level).unwrap()
    }

    /// Checkpoints everything before the current segment, as a shard whose flushes keep up
    /// would, so slots are recycled and the file stays at its size.
    fn checkpoint(&mut self) {
        let written = self.wal.written();
        self.wal.checkpoint(written).unwrap();
    }

    /// One group of `g` commits at `level`; returns once every member's level is met.
    fn group(&mut self, g: usize, level: Durability) {
        let mut last = None;
        for _ in 0..g {
            last = Some(self.append_one(level));
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
        self.checkpoint();
        black_box(t);
    }

    /// One pipelined GroupSync group: submits this group's sync and waits only for the group
    /// `DEPTH` behind it, so the shard keeps building groups while syncs run.
    fn pipelined_group(&mut self, g: usize) {
        for _ in 0..g {
            self.append_one(Durability::GroupSync);
        }
        let c = self.wal.submit_sync().unwrap();
        self.in_flight.push_back(c);
        if self.in_flight.len() > DEPTH {
            let done = self.in_flight.pop_front().unwrap().wait().unwrap();
            black_box(done);
        }
        self.checkpoint();
    }

    fn drain(&mut self) {
        while let Some(c) = self.in_flight.pop_front() {
            c.wait().unwrap();
        }
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
    // Pipelined: the latency of one group is the time from its submit until its own sync
    // completes, measured with DEPTH groups in flight.
    let mut b = Bench::new("pipelined-latency");
    let mut samples = Vec::new();
    let mut starts = VecDeque::new();
    for _ in 0..800 {
        for _ in 0..16 {
            b.append_one(Durability::GroupSync);
        }
        let c = b.wal.submit_sync().unwrap();
        starts.push_back(Instant::now());
        b.in_flight.push_back(c);
        if b.in_flight.len() > DEPTH {
            b.in_flight.pop_front().unwrap().wait().unwrap();
            samples.push(starts.pop_front().unwrap().elapsed());
        }
        b.checkpoint();
    }
    b.drain();
    percentiles(
        &format!("commit latency group_sync_pipelined g=16 depth={DEPTH}"),
        samples,
    );
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
    for g in [16usize, 128] {
        let mut b = Bench::new(&format!("pipelined-{g}"));
        grp.throughput(Throughput::Elements(g as u64));
        grp.sample_size(20).measurement_time(Duration::from_secs(5));
        grp.bench_with_input(
            BenchmarkId::new("group_sync_pipelined", g),
            &g,
            |bench, &g| {
                bench.iter(|| b.pipelined_group(g));
            },
        );
        b.drain();
    }
    grp.finish();
}

criterion_group!(benches, bench_commit);
criterion_main!(benches);
