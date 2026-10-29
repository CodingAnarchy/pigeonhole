//! The D194 stale-tail index changes how reads pass a finished column's superseded memtable
//! versions, never what they return. Random overwrite-heavy histories (puts at newer, older
//! and equal timestamps, cell deletes at a put's timestamp, column deletes, merge operands
//! and counter buckets), partly flushed, are read at several snapshots with several specs,
//! once over memtables with the index (jumping) and once without (stepping,
//! `EngineOptions::memtable_tail_index` off), and the results must be identical. The jump
//! count makes sure the jumps ran. SST column skips (ICR 0041) run in both, and
//! `pigeonhole-sst`'s tests check them against stepping. One application-owned shard over
//! `SimVfs`; prints the seed on failure.

mod common;

use std::future::Future;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::{
    Engine, EngineShard, FamilyKind, FamilyOptions, QualifierFilter, ReadSpec, ScanSpec, Snapshot,
    TableInfo, ValueRef, WriteBatch,
};
use pigeonhole_format::Durability;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/tail_index.phdb";
const ROWS: u64 = 4;
const QUALS: u64 = 6;

struct Rig {
    db: Arc<Engine>,
    shard: EngineShard,
    t: Arc<TableInfo>,
    vfs: Arc<SimVfs>,
}

/// The reader process of D277's check: it reads the writer's memtables through the shared
/// arena, jumping by the stale tails the writer stored in the nodes.
const READER: pigeonhole_io::ProcessId = pigeonhole_io::ProcessId {
    pid: 4242,
    start_time: 1,
};

/// A read's result, comparable: (family, qualifier, timestamp, value) per cell.
type Cells = Vec<(u32, Vec<u8>, u64, Vec<u8>)>;

impl Rig {
    /// One shard over a fresh `SimVfs`, its memtables with the stale-tail index or without.
    fn open(seed: u64, tail_index: bool) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = common::options(Arc::clone(&vfs), 1, 16 << 20);
        o.tablet_changes = false;
        o.compaction.l0_trigger = u32::MAX;
        o.memtable_tail_index = tail_index;
        let (db, mut shards) = Engine::open_application_owned(Path::new(DB), o).unwrap();
        let mut shard = shards.remove(0);
        while shard.run_once(u64::MAX) {}
        let t = db
            .create_table(
                "t",
                &[
                    ("f".into(), FamilyOptions::default().max_versions(1)),
                    ("v".into(), FamilyOptions::default().max_versions(3)),
                    (
                        "m".into(),
                        FamilyOptions::default().merge_operator("pigeonhole.i64_add"),
                    ),
                    (
                        "c".into(),
                        FamilyOptions::default()
                            .merge_operator("pigeonhole.i64_add")
                            .kind(FamilyKind::Counter),
                    ),
                ],
            )
            .unwrap();
        Self { db, shard, t, vfs }
    }

    fn wait<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = Pin::new(&mut f).poll(&mut cx) {
                return r;
            }
            self.shard.run_once(u64::MAX);
        }
    }

    fn commit(&mut self, wb: WriteBatch) {
        let pending = self.db.submit(wb, Some(Durability::Buffered)).unwrap();
        self.wait(pending).unwrap();
    }

    fn flush(&mut self) {
        let flush = self.db.flush_pending().unwrap();
        self.wait(flush).unwrap();
        while self.shard.run_once(u64::MAX) {}
    }

    fn specs(&self) -> Vec<ReadSpec> {
        let spec = |f: &dyn Fn(&mut ReadSpec)| {
            let mut s = ReadSpec::default();
            f(&mut s);
            s
        };
        vec![
            spec(&|s| s.versions = 1),
            spec(&|s| s.versions = 2),
            spec(&|s| s.versions = 0),
            spec(&|s| s.time_range = Some((20, 60))),
            spec(&|s| s.columns_per_row = 2),
            spec(&|s| s.qualifiers = QualifierFilter::Prefix(b"q1".to_vec())),
        ]
    }

    /// Every row read and a full scan, under every spec.
    fn read_all(&self, snap: &Snapshot) -> Vec<Cells> {
        read_all_on(&self.db, &self.t, snap, self.specs())
    }
}

/// Every spec's row reads and scan of `t` through `db` (the writer, or a reader process).
fn read_all_on(db: &Engine, t: &TableInfo, snap: &Snapshot, specs: Vec<ReadSpec>) -> Vec<Cells> {
    let mut out = Vec::new();
    for spec in specs {
        for r in 0..ROWS {
            let row = db.read_row(snap, t.id, &row_key(r), &spec).unwrap();
            out.push(row.map_or_else(Vec::new, |row| {
                row.cells
                    .iter()
                    .map(|c| {
                        (
                            c.family.0,
                            row.qualifier(c).to_vec(),
                            c.data.timestamp(),
                            common::value_bytes(c.data.value()),
                        )
                    })
                    .collect()
            }));
        }
        let mut scan = db
            .scan(snap, t.id, {
                let mut scan = ScanSpec::new(Bound::Unbounded, Bound::Unbounded);
                scan.read = spec.clone();
                scan
            })
            .unwrap();
        let mut cells = Vec::new();
        while scan.next_row().unwrap() {
            let row = scan.row().to_vec();
            while let Some(c) = scan.next_cell().unwrap() {
                cells.push((
                    c.family.0,
                    [row.as_slice(), b"/", c.qualifier].concat(),
                    c.ts,
                    c.stored.to_vec(),
                ));
            }
        }
        out.push(cells);
    }
    out
}

fn row_key(r: u64) -> Vec<u8> {
    format!("row{r}").into_bytes()
}

/// A seeded xorshift.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// What one run of a seeded history read: each snapshot's reads, then the final state's
/// reads by the writer and by a reader process, and the memtable jumps the reader took.
struct Played {
    snaps: Vec<Vec<Cells>>,
    writer: Vec<Cells>,
    reader: Vec<Cells>,
    reader_jumps: u64,
}

/// One seeded history over memtables with the stale-tail index (`tail_index`), whose reads
/// jump a finished column's superseded versions, or without it, whose reads step over them.
/// The same writes and flushes either way.
fn play(seed: u64, tail_index: bool) -> Played {
    let mut rig = Rig::open(seed, tail_index);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let fams: Vec<_> = rig.t.families.iter().map(|f| f.id).collect();
    let (f, v, m, c) = (fams[0], fams[1], fams[2], fams[3]);
    let mut snaps: Vec<Snapshot> = Vec::new();
    let mut ts = 10;
    for step in 0..400i64 {
        let row = row_key(rng.below(ROWS));
        let qual = format!("q{}", rng.below(QUALS)).into_bytes();
        let mut wb = WriteBatch::new();
        let t = rig.t.id;
        // Mostly newer timestamps (the overwrite pattern), sometimes an older or equal one.
        ts += 1;
        let at = match rng.below(8) {
            0 => ts - 1 - rng.below(ts - 1).min(30),
            1 => ts - 1,
            _ => ts,
        };
        let value = format!("{seed}:{step}").into_bytes();
        match rng.below(20) {
            0 => wb
                .delete_cell(t, [f, v][rng.below(2) as usize], &row, &qual, at)
                .unwrap(),
            1 => wb
                .delete_column(t, [f, v][rng.below(2) as usize], &row, &qual, Some(at))
                .unwrap(),
            2 | 3 => wb
                .merge(t, m, &row, &qual, ValueRef::I64(rng.below(5) as i64))
                .unwrap(),
            4 => wb
                .merge_at(t, c, &row, &qual, at / 4, ValueRef::I64(1))
                .unwrap(),
            _ => {
                // The merge family's base must be an i64 (pigeonhole.i64_add).
                match rng.below(3) {
                    0 => wb.put(t, m, &row, &qual, Some(at), ValueRef::I64(step)),
                    i => wb.put(
                        t,
                        [f, v][i as usize - 1],
                        &row,
                        &qual,
                        Some(at),
                        ValueRef::Bytes(&value),
                    ),
                }
                .unwrap();
            }
        }
        rig.commit(wb);
        if rng.below(40) == 0 {
            snaps.push(rig.db.snapshot().unwrap());
            if snaps.len() > 4 {
                snaps.remove(0);
            }
        }
        if step == 150 || step == 300 {
            rig.flush();
        }
    }
    snaps.push(rig.db.snapshot().unwrap());
    let snap_reads = snaps.iter().map(|snap| rig.read_all(snap)).collect();
    drop(snaps);

    // D277: a reader process reads the same memtables through the shared arena, jumping by
    // the tails the writer stored in the nodes (with the index) or stepping (without).
    let writer = pigeonhole_io::Vfs::current_process(&*rig.vfs);
    let writer_snap = rig.db.snapshot().unwrap();
    let writer_reads = rig.read_all(&writer_snap);
    drop(writer_snap);
    rig.vfs.enter_process(READER);
    let reader = Engine::open_reader(
        Path::new(DB),
        common::options(Arc::clone(&rig.vfs), 1, 16 << 20),
    )
    .unwrap();
    let snap = reader.snapshot().unwrap();
    let before = Engine::tail_skips();
    let reader_reads = read_all_on(&reader, &rig.t, &snap, rig.specs());
    let reader_jumps = Engine::tail_skips() - before;
    drop(snap);
    drop(reader);
    rig.vfs.enter_process(writer);

    let Rig { db, mut shard, .. } = rig;
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
    Played {
        snaps: snap_reads,
        writer: writer_reads,
        reader: reader_reads,
        reader_jumps,
    }
}

/// One seeded history, played with the index and without: every read returns the same
/// cells, the writer's and a reader process's alike. Returns the jumps the reader process
/// took with the index (D277).
fn history(seed: u64) -> u64 {
    let jumping = play(seed, true);
    let stepping = play(seed, false);
    assert_eq!(
        stepping.reader_jumps, 0,
        "seed {seed}: jumps without the index"
    );
    for (i, (a, b)) in jumping.snaps.iter().zip(&stepping.snaps).enumerate() {
        for (j, (a, b)) in a.iter().zip(b).enumerate() {
            assert_eq!(a, b, "seed {seed}, snapshot {i}, read {j}");
        }
    }
    for (j, (((a, b), w), v)) in jumping
        .reader
        .iter()
        .zip(&stepping.reader)
        .zip(&jumping.writer)
        .zip(&stepping.writer)
        .enumerate()
    {
        assert_eq!(a, b, "seed {seed}, reader read {j}: jumping vs stepping");
        assert_eq!(a, w, "seed {seed}, reader read {j}: reader vs writer");
        assert_eq!(w, v, "seed {seed}, writer read {j}: jumping vs stepping");
    }
    jumping.reader_jumps
}

/// One test: the jump counters are process-wide.
#[test]
fn jumping_reads_return_what_stepping_reads_return() {
    // At least the default 12 seeds, whatever `PIGEONHOLE_SEEDS` asks (#810): the counts
    // below are sums over the seeds, and one seed's vary a lot (seed 1 makes 44 memtable
    // jumps), so fewer seeds would fail the thresholds with the index working. The 12 take
    // well under a second.
    const MIN_SEEDS: u64 = 12;
    let seeds: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MIN_SEEDS)
        .max(MIN_SEEDS);
    let before = Engine::tail_skips();
    let sst_before = Engine::sst_skips();
    let mut reader_jumps = 0;
    for seed in 1..=seeds {
        reader_jumps += history(seed);
    }
    // D277: the reader process jumps too, by the tails in the shared nodes.
    assert!(
        reader_jumps > 100,
        "only {reader_jumps} memtable jumps in the reader process: the shared tails are unused"
    );
    let jumps = Engine::tail_skips() - before;
    assert!(
        jumps > 1_000,
        "only {jumps} memtable jumps: the index is not used"
    );
    let sst = Engine::sst_skips() - sst_before;
    assert!(sst > 1_000, "only {sst} SST column skips (ICR 0041)");
}
