//! The D194 stale-tail index changes how reads pass a finished column's superseded memtable
//! versions, never what they return. Random overwrite-heavy histories (puts at newer, older
//! and equal timestamps, cell deletes at a put's timestamp, column deletes, merge operands
//! and counter buckets), partly flushed, are read at several snapshots with several specs,
//! once jumping and once stepping over the same memtables (`Engine::set_tail_index`), and
//! the results must be identical. The jump count makes sure the jumps ran. One
//! application-owned shard over `SimVfs`; prints the seed on failure.

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
}

/// A read's result, comparable: (family, qualifier, timestamp, value) per cell.
type Cells = Vec<(u32, Vec<u8>, u64, Vec<u8>)>;

impl Rig {
    fn open(seed: u64) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o = common::options(vfs, 1, 16 << 20);
        o.tablet_changes = false;
        o.compaction.l0_trigger = u32::MAX;
        o.memtable_tail_index = true;
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
        Self { db, shard, t }
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
        let mut out = Vec::new();
        for spec in self.specs() {
            for r in 0..ROWS {
                let row = self
                    .db
                    .read_row(snap, self.t.id, &row_key(r), &spec)
                    .unwrap();
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
            let mut scan = self
                .db
                .scan(snap, self.t.id, {
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

fn history(seed: u64) {
    let mut rig = Rig::open(seed);
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
    for (i, snap) in snaps.iter().enumerate() {
        Engine::set_tail_index(true);
        let jumping = rig.read_all(snap);
        Engine::set_tail_index(false);
        let stepping = rig.read_all(snap);
        Engine::set_tail_index(true);
        for (j, (a, b)) in jumping.iter().zip(&stepping).enumerate() {
            assert_eq!(a, b, "seed {seed}, snapshot {i}, read {j}");
        }
    }
    drop(snaps);
    let Rig { db, mut shard, .. } = rig;
    db.close().unwrap();
    while shard.closed().is_none() {
        shard.run_once(u64::MAX);
    }
}

/// One test, so nothing else in this binary flips the process-wide switch meanwhile.
#[test]
fn jumping_reads_return_what_stepping_reads_return() {
    let seeds: u64 = std::env::var("PIGEONHOLE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    let before = Engine::tail_skips();
    for seed in 1..=seeds {
        history(seed);
    }
    let jumps = Engine::tail_skips() - before;
    assert!(
        jumps > 1_000,
        "only {jumps} memtable jumps: the index is not used"
    );
}
