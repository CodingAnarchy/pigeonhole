//! Compaction throughput (a two-level merge, MB/s of input, incompressible values),
//! resolver throughput (cells/s over a merge of SSTs, warm and cold cache), and the read
//! amplification of an unfolded counter (one point get over N `incr` operands).
#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use pigeonhole_cache::{BlockCache, Priority};
use pigeonhole_compaction::{
    CellResolver, CompactionJob, CompactionTask, Error, GcPolicy, I64Add, JobContext, JobPoll,
    KeyRange, MergingCursor, ResolveOptions, TaskKind,
};
use pigeonhole_format::key::{Kind, encode_key};
use pigeonhole_format::manifest::{FamilyOptions, SstMeta};
use pigeonhole_format::{Cursor, FamilyId, SstId, TableId, TabletId};
use pigeonhole_io::VfsRef;
use pigeonhole_io::sim::SimVfs;
use pigeonhole_pager::Pager;
use pigeonhole_sst::{ReadOptions, ScanFilter, SstIter, SstReader, SstWriter, SstWriterOptions};

const ROWS: u32 = 20_000;
const VALUE: usize = 100;

struct Fixture {
    pager: Arc<Pager>,
    cache: Arc<BlockCache>,
    family: FamilyOptions,
    inputs: Vec<(SstMeta, Arc<SstReader>)>,
    bytes: u64,
}

/// Two overlapping levels: L1 holds the newer version of every other row, L2 two versions of
/// every row.
fn fixture() -> Fixture {
    let vfs: VfsRef = SimVfs::new(1);
    let pager = Arc::new(Pager::create(&vfs, "/bench.phdb".as_ref()).unwrap());
    let cache = Arc::new(BlockCache::new(256 << 20, 4));
    let family = FamilyOptions::default();
    let mut inputs = Vec::new();
    let mut bytes = 0;
    for (id, (rows, versions, seqno)) in [
        ((0..ROWS).step_by(2), vec![30u64], 3u64),
        ((0..ROWS).step_by(1), vec![20, 10], 1),
    ]
    .into_iter()
    .enumerate()
    {
        let opts = SstWriterOptions::for_family(&family, TableId(1), FamilyId(1), TabletId(1));
        let extent = pager.allocate(64 << 20).unwrap();
        let mut w = SstWriter::new(pager.file().clone(), extent, SstId(id as u64 + 1), opts);
        let mut k = Vec::new();
        let mut value = vec![0u8; VALUE + 1];
        let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ id as u64;
        for row in rows {
            // Incompressible: a fresh xorshift payload per entry.
            for b in &mut value[1..] {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *b = x as u8;
            }
            for (i, &ts) in versions.iter().enumerate() {
                k.clear();
                encode_key(
                    &mut k,
                    format!("row{row:08}").as_bytes(),
                    b"col",
                    ts,
                    seqno + i as u64,
                    Kind::Put,
                )
                .unwrap();
                w.add(&k, &value).unwrap();
                bytes += (k.len() + value.len()) as u64;
            }
        }
        let meta = w.finish().unwrap();
        let reader = Arc::new(
            SstReader::open(pager.file().clone(), &meta, cache.clone(), Priority::Normal).unwrap(),
        );
        inputs.push((meta, reader));
    }
    Fixture {
        pager,
        cache,
        family,
        inputs,
        bytes,
    }
}

fn compaction(c: &mut Criterion) {
    let f = fixture();
    let ids = Arc::new(AtomicU64::new(100));
    let mut g = c.benchmark_group("compaction");
    g.throughput(Throughput::Bytes(f.bytes));
    g.sample_size(20);
    g.bench_function("two_level_merge", |b| {
        b.iter_batched(
            || {
                let task = CompactionTask {
                    tablet: TabletId(1),
                    family: FamilyId(1),
                    range: KeyRange::all(),
                    subranges: vec![KeyRange::all()],
                    inputs: vec![(1, vec![SstId(1)]), (2, vec![SstId(2)])],
                    output_level: 2,
                    kind: TaskKind::Rewrite,
                };
                let mut gc = GcPolicy::new(vec![], 1_000, true);
                gc.min_ts_above = u64::MAX;
                let ctx = JobContext::new(
                    TableId(1),
                    f.family.clone(),
                    f.pager.clone(),
                    f.cache.clone(),
                    ids.clone(),
                    Arc::new(AtomicU32::new(1)),
                    gc,
                );
                CompactionJob::new(task, f.inputs.iter().map(|s| s.1.clone()).collect(), ctx)
            },
            |mut job| {
                while job.run(u64::MAX).unwrap() == JobPoll::Pending {}
                let out = job.finish().unwrap();
                for (_, m) in &out.added {
                    f.pager.abandon(m.extent);
                }
                black_box(out.added.len())
            },
            BatchSize::PerIteration,
        )
    });
    g.finish();
}

struct Src(SstIter);

impl Cursor for Src {
    type Error = Error;
    fn valid(&self) -> bool {
        self.0.valid()
    }
    fn key(&self) -> &[u8] {
        self.0.key()
    }
    fn value(&self) -> &[u8] {
        self.0.value()
    }
    fn seek_to_first(&mut self) -> Result<(), Error> {
        Ok(self.0.seek_to_first()?)
    }
    fn seek(&mut self, target: &[u8]) -> Result<(), Error> {
        Ok(self.0.seek(target)?)
    }
    fn next(&mut self) -> Result<(), Error> {
        Ok(self.0.next()?)
    }
    fn skip_row(&mut self) -> Result<(), Error> {
        Ok(self.0.skip_row()?)
    }
}

fn scan_all(inputs: &[Arc<SstReader>]) -> u32 {
    let sources = inputs
        .iter()
        .map(|s| Src(s.iter(ScanFilter::all(), ReadOptions::default())))
        .collect();
    let mut r = CellResolver::new(
        MergingCursor::new(sources),
        ResolveOptions::new(u64::MAX, 0),
    );
    r.seek(b"").unwrap();
    let mut n = 0u32;
    while let Some(cell) = r.next_cell().unwrap() {
        black_box(cell.value);
        n += 1;
    }
    n
}

fn resolver(c: &mut Criterion) {
    let f = fixture();
    let mut g = c.benchmark_group("resolver");
    g.throughput(Throughput::Elements(u64::from(ROWS)));
    let warm: Vec<_> = f.inputs.iter().map(|s| s.1.clone()).collect();
    g.bench_function("scan_latest_two_ssts", |b| {
        b.iter(|| assert_eq!(scan_all(&warm), ROWS))
    });
    // Cold: every block is read, verified and decompressed again (readers opened over an
    // empty cache each iteration).
    g.bench_function("scan_latest_two_ssts_cold", |b| {
        b.iter_batched(
            || {
                let cache = Arc::new(BlockCache::new(256 << 20, 4));
                f.inputs
                    .iter()
                    .map(|s| {
                        Arc::new(
                            SstReader::open(
                                f.pager.file().clone(),
                                &s.0,
                                cache.clone(),
                                Priority::Normal,
                            )
                            .unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            },
            |cold| assert_eq!(scan_all(&cold), ROWS),
            BatchSize::PerIteration,
        )
    });
    g.finish();
}

/// One counter column holding `n` unfolded `incr` operands (compaction does not fold across
/// timestamps until #34): the cost of a point get grows with `n`.
fn counter(c: &mut Criterion) {
    let vfs: VfsRef = SimVfs::new(2);
    let pager = Pager::create(&vfs, "/counter.phdb".as_ref()).unwrap();
    let cache = Arc::new(BlockCache::new(64 << 20, 1));
    let family = FamilyOptions::default();
    let mut g = c.benchmark_group("counter_get");
    for n in [1u64, 100, 10_000] {
        let opts = SstWriterOptions::for_family(&family, TableId(1), FamilyId(1), TabletId(1));
        let extent = pager.allocate(8 << 20).unwrap();
        let mut w = SstWriter::new(pager.file().clone(), extent, SstId(100 + n), opts);
        let mut k = Vec::new();
        for i in (1..=n).rev() {
            k.clear();
            encode_key(&mut k, b"page", b"hits", i, i, Kind::Merge).unwrap();
            w.add(&k, &[&[1u8][..], &1i64.to_le_bytes()].concat())
                .unwrap();
        }
        let meta = w.finish().unwrap();
        let sst = Arc::new(
            SstReader::open(pager.file().clone(), &meta, cache.clone(), Priority::Normal).unwrap(),
        );
        g.bench_function(format!("operands_{n}"), |b| {
            b.iter(|| {
                let mut o = ResolveOptions::new(u64::MAX, 0);
                o.merge = Some(Arc::new(I64Add));
                let src = Src(sst.iter(ScanFilter::all(), ReadOptions::default()));
                let mut r = CellResolver::new(MergingCursor::new(vec![src]), o);
                r.seek_column(b"page", b"hits").unwrap();
                black_box(r.next_cell().unwrap().unwrap().value.len())
            })
        });
    }
    g.finish();
}

criterion_group!(benches, compaction, resolver, counter);
criterion_main!(benches);
