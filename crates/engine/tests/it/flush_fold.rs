//! D287 (#713): a counter family's flush combines its memtable's operands as a compaction
//! does (D186), guarded by the slot's other sources, all of which lie below or above the
//! memtable's seqnos. Before it, a flush wrote every operand, and a hot counter's read folded
//! all of them across L0. One engine-owned shard over `SimVfs`, compaction off, so what the
//! SSTs hold is the flush's doing.

use crate::common;

use std::path::Path;
use std::sync::Arc;

use pigeonhole_engine::{
    Engine, EngineOptions, FamilyId, FamilyOptions, FlushGcMutation, ReadSpec, Snapshot, TableId,
    ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::FamilyKind;
use pigeonhole_io::sim::SimVfs;

const DB: &str = "/db/flush_fold.phdb";

struct Rig {
    db: Arc<Engine>,
    t: TableId,
    c: FamilyId,
}

impl Rig {
    fn open(seed: u64, mutation: FlushGcMutation) -> Self {
        let vfs = SimVfs::new(seed);
        let mut o: EngineOptions = common::options(Arc::clone(&vfs), 1, 16 << 20);
        o.pin_threads = false;
        o.compaction.l0_trigger = u32::MAX;
        // WAL segments large enough that no checkpoint unpin flushes a memtable early (#137):
        // the tests' flushes are their own.
        o.wal.segment_size = 4 << 20;
        let db = Engine::open(Path::new(DB), o).unwrap();
        db.mutate_flush_gc(mutation);
        let family = FamilyOptions::default()
            .merge_operator("pigeonhole.i64_add")
            .kind(FamilyKind::Counter);
        let info = db.create_table("t", &[("c".into(), family)]).unwrap();
        let c = info.family("c").unwrap().id;
        Self { db, t: info.id, c }
    }

    fn incr(&self, d: i64) {
        let mut wb = WriteBatch::new();
        wb.merge(self.t, self.c, b"r", b"n", ValueRef::I64(d))
            .unwrap();
        self.db.commit(wb, None).unwrap();
    }

    fn delete(&self) {
        let mut wb = WriteBatch::new();
        wb.delete_column(self.t, self.c, b"r", b"n", None).unwrap();
        self.db.commit(wb, None).unwrap();
    }

    /// The counter's value at `snap` (0 when it reads nothing).
    fn sum_at(&self, snap: &Snapshot) -> i64 {
        let mut spec = ReadSpec::default();
        spec.families = vec![self.c];
        self.db
            .read_row(snap, self.t, b"r", &spec)
            .unwrap()
            .map_or(0, |r| {
                r.cells
                    .iter()
                    .map(|cell| match cell.data.value() {
                        ValueRef::I64(v) => v,
                        v => panic!("not an i64: {v:?}"),
                    })
                    .sum()
            })
    }

    fn sum(&self) -> i64 {
        self.sum_at(&self.db.snapshot().unwrap())
    }

    /// Entries the counter family stores (memtables and SSTs, unresolved).
    fn stored(&self) -> usize {
        let snap = self.db.snapshot().unwrap();
        self.db
            .raw_entries(&snap)
            .unwrap()
            .iter()
            .filter(|e| e.family == self.c)
            .count()
    }
}

/// 2,000 increments of one counter, then a flush: its L0 SST holds one combined operand, and
/// the counter reads the same sum before and after.
#[test]
fn a_flush_combines_a_counters_operands_into_one() {
    let rig = Rig::open(287, FlushGcMutation::None);
    for i in 1..=2_000 {
        rig.incr(i);
    }
    let want = (1..=2_000i64).sum::<i64>();
    assert_eq!(rig.sum(), want);
    assert_eq!(rig.stored(), 2_000, "the memtable holds every operand");
    rig.db.flush().unwrap();
    assert_eq!(rig.stored(), 1, "the flush combined the operands");
    assert_eq!(rig.sum(), want);
}

/// A snapshot taken mid-run is a read point: the flush combines on each side of it (two
/// entries), and the snapshot still reads the older sum.
#[test]
fn a_snapshot_splits_the_combined_operands() {
    let rig = Rig::open(288, FlushGcMutation::None);
    for _ in 0..1_000 {
        rig.incr(1);
    }
    let snap = rig.db.snapshot().unwrap();
    for _ in 0..1_000 {
        rig.incr(2);
    }
    rig.db.flush().unwrap();
    assert_eq!(rig.stored(), 2, "one operand per stripe");
    assert_eq!(rig.sum_at(&snap), 1_000);
    assert_eq!(rig.sum(), 3_000);
}

/// A delete between operands (a counter delete hides only lower seqnos, D186): the flush
/// combines on each side of it, and every read point reads what it read before the flush.
#[test]
fn a_delete_between_operands_keeps_every_read_point() {
    let rig = Rig::open(289, FlushGcMutation::None);
    for _ in 0..300 {
        rig.incr(5);
    }
    let before = rig.db.snapshot().unwrap();
    rig.delete();
    let deleted = rig.db.snapshot().unwrap();
    for _ in 0..300 {
        rig.incr(7);
    }
    let reads = |rig: &Rig| (rig.sum_at(&before), rig.sum_at(&deleted), rig.sum());
    assert_eq!(reads(&rig), (1_500, 0, 2_100));
    rig.db.flush().unwrap();
    assert_eq!(reads(&rig), (1_500, 0, 2_100));
    assert!(
        rig.stored() <= 4,
        "the runs on either side of the delete combine: {}",
        rig.stored()
    );
}

/// The guard has teeth: told of another source of the slot whose seqnos span the memtable's
/// (a test hook), the flush keeps every operand, as D186 requires when a delete there could
/// fall between two of them.
#[test]
fn a_source_inside_the_memtables_seqnos_keeps_every_operand() {
    let rig = Rig::open(290, FlushGcMutation::CounterSourceInside);
    for _ in 0..500 {
        rig.incr(1);
    }
    rig.db.flush().unwrap();
    assert_eq!(rig.stored(), 500);
    assert_eq!(rig.sum(), 500);
}

/// A second flush of the same counter writes a second L0 SST with one operand of its own,
/// and the two still read as the sum (each flush's other sources include the first SST,
/// entirely below its memtable's seqnos).
#[test]
fn consecutive_flushes_each_combine_their_own_operands() {
    let rig = Rig::open(291, FlushGcMutation::None);
    for _ in 0..400 {
        rig.incr(1);
    }
    rig.db.flush().unwrap();
    for _ in 0..400 {
        rig.incr(3);
    }
    rig.db.flush().unwrap();
    assert_eq!(rig.stored(), 2);
    assert_eq!(rig.sum(), 1_600);
}

// ---- a decided cross-shard share against a counter slot's freeze (simp724's review) ----

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use pigeonhole_engine::EngineShard;
use pigeonhole_format::Durability;

/// Three application-owned shards; table `t` with a counter family `c` and a plain `f`, split
/// so rows `a..` are on shard 0, `b..` on 1 and `c..` on 2, each shard run on its own.
struct Shards {
    vfs: Arc<SimVfs>,
    db: Arc<Engine>,
    shards: Vec<EngineShard>,
    t: TableId,
    c: FamilyId,
    f: FamilyId,
}

fn poll<F: Future + Unpin>(f: &mut F) -> Option<F::Output> {
    match Pin::new(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

impl Shards {
    fn open_on(vfs: &Arc<SimVfs>, create: bool) -> Self {
        let mut o: EngineOptions = common::options(Arc::clone(vfs), 3, 16 << 20);
        o.pin_threads = false;
        o.tablet_changes = true;
        o.balance_interval_nanos = 0;
        o.compaction.l0_trigger = u32::MAX;
        o.wal.segment_size = 4 << 20;
        let (db, shards) = Engine::open_application_owned(Path::new(DB), o).expect("open");
        let info = match db.table("t") {
            Some(t) => t,
            None => {
                assert!(create);
                let counter = FamilyOptions::default()
                    .merge_operator("pigeonhole.i64_add")
                    .kind(FamilyKind::Counter);
                db.create_table(
                    "t",
                    &[
                        ("c".into(), counter),
                        ("f".into(), FamilyOptions::default()),
                    ],
                )
                .unwrap()
            }
        };
        let mut s = Self {
            vfs: Arc::clone(vfs),
            c: info.family("c").unwrap().id,
            f: info.family("f").unwrap().id,
            t: info.id,
            db,
            shards,
        };
        if create {
            for at in [&b"b"[..], b"c"] {
                let m = s.db.split_tablet_pending(s.t, at).unwrap();
                s.drive(m).unwrap();
            }
            for (row, to) in [(&b"a"[..], 0u16), (b"b", 1), (b"c", 2)] {
                let owner =
                    s.db.snapshot()
                        .unwrap()
                        .view()
                        .tablets()
                        .ranges(s.t)
                        .iter()
                        .rfind(|r| r.2.as_slice() <= row)
                        .map(|r| r.1)
                        .unwrap();
                if owner != to {
                    let m = s.db.move_tablet_pending(s.t, row, to).unwrap();
                    s.drive(m).unwrap();
                }
            }
        }
        s
    }

    fn run(&mut self, i: usize) {
        let now = pigeonhole_io::Vfs::monotonic_nanos(&*self.vfs);
        self.shards[i].run_once(now + 1_000);
    }

    fn step(&mut self) {
        for i in 0..3 {
            self.run(i);
        }
    }

    fn drive<F: Future + Unpin>(&mut self, mut f: F) -> F::Output {
        for _ in 0..100_000 {
            if let Some(v) = poll(&mut f) {
                return v;
            }
            self.step();
        }
        panic!("never resolved");
    }

    /// The counter `b-n` (on shard 1) as of now.
    fn sum(&self) -> i64 {
        let mut spec = ReadSpec::default();
        spec.families = vec![self.c];
        self.db
            .read_row(&self.db.snapshot().unwrap(), self.t, b"b-n", &spec)
            .unwrap()
            .map_or(0, |r| {
                r.cells
                    .iter()
                    .map(|cell| match cell.data.value() {
                        ValueRef::I64(v) => v,
                        v => panic!("not an i64: {v:?}"),
                    })
                    .sum()
            })
    }

    fn stored(&self) -> usize {
        let snap = self.db.snapshot().unwrap();
        self.db
            .raw_entries(&snap)
            .unwrap()
            .iter()
            .filter(|e| e.family == self.c)
            .count()
    }

    fn incr(&self, d: i64) -> pigeonhole_engine::PendingCommit {
        let mut wb = WriteBatch::new();
        wb.merge(self.t, self.c, b"b-n", b"q", ValueRef::I64(d))
            .unwrap();
        self.db.submit(wb, Some(Durability::Buffered)).unwrap()
    }

    /// A cross-shard commit over shards 0 (its coordinator: the first row), 1 (the counter)
    /// and, with `three`, 2.
    fn cross(&self, d: i64, three: bool) -> pigeonhole_engine::PendingCommit {
        let mut wb = WriteBatch::new();
        wb.put(self.t, self.f, b"a-x", b"q", None, ValueRef::Bytes(b"x"))
            .unwrap();
        wb.merge(self.t, self.c, b"b-n", b"q", ValueRef::I64(d))
            .unwrap();
        if three {
            wb.put(self.t, self.f, b"c-x", b"q", None, ValueRef::Bytes(b"x"))
                .unwrap();
        }
        self.db.submit(wb, Some(Durability::Buffered)).unwrap()
    }
}

/// D287's point 3, the hold: a cross-shard commit is decided and applied on the counter's
/// shard (1), but shard 2 has not applied its share, so the commit's seqno stays held. The
/// counter's memtable, with 50 singles above that seqno, defers its freeze (it may not
/// freeze above `durable_seqno`) rather than flush while a decided share is unreleased; once
/// shard 2 applies, it flushes, the operands combine, and the counter reads its sum, also
/// after a reopen.
#[test]
fn a_counter_slot_defers_its_freeze_while_a_decided_share_is_held() {
    let vfs = SimVfs::new(292);
    let mut s = Shards::open_on(&vfs, true);
    let mut x = s.cross(1_000, true);
    s.run(0); // PREPAREs out
    s.run(1); // logged, voted
    s.run(2);
    s.run(0); // decided (its apply seqno held), DECISION written, Decides out
    s.run(1); // shard 1 applies its share; shard 2 has not
    // 50 singles in one group: few enough that the group fits the arena whatever admission
    // reserves per member (#749).
    let mut singles: Vec<_> = (0..50).map(|_| s.incr(1)).collect();
    for _ in 0..8 {
        s.run(1);
    }
    // Applied, but not answered: a commit is answered once visible, and visibility stops
    // below the held seqno (D278).
    assert!(
        singles.iter_mut().all(|pc| poll(pc).is_none()),
        "a single answered while a decided share below it was held"
    );
    let mut flush = s.db.flush_pending().unwrap();
    for _ in 0..50 {
        s.run(0);
        s.run(1);
    }
    assert!(
        poll(&mut flush).is_none(),
        "the flush finished with a decided share held"
    );
    assert_eq!(
        s.stored(),
        51,
        "nothing froze: every operand is in the memtable"
    );
    assert!(
        poll(&mut x).is_none(),
        "the cross-shard commit answered before shard 2 applied"
    );
    s.drive(&mut flush).unwrap();
    s.drive(&mut x).unwrap();
    for pc in &mut singles {
        s.drive(pc).unwrap();
    }
    assert_eq!(
        s.stored(),
        1,
        "the share and the singles combined at the flush"
    );
    assert_eq!(s.sum(), 1_050);
    s.db.close().unwrap();
    while s.shards.iter_mut().any(|sh| sh.closed().is_none()) {
        s.step();
    }
    drop(s);
    let s = Shards::open_on(&vfs, false);
    assert_eq!(s.sum(), 1_050, "after a reopen");
}

/// The other order: the counter's memtable freezes and flushes first, then a cross-shard
/// commit decided afterwards lands its share above the flushed memtable's seqnos (a later
/// apply seqno), in the next memtable. Each flush combines its own operands, and the
/// second flush's other sources (the first SST) lie entirely below it.
#[test]
fn a_share_decided_after_a_flush_lands_above_it() {
    let vfs = SimVfs::new(293);
    let mut s = Shards::open_on(&vfs, true);
    for _ in 0..200 {
        let pc = s.incr(1);
        s.drive(pc).unwrap();
    }
    let f = s.db.flush_pending().unwrap();
    s.drive(f).unwrap();
    assert_eq!(s.stored(), 1);
    let x = s.cross(1_000, false);
    s.drive(x).unwrap();
    let f = s.db.flush_pending().unwrap();
    s.drive(f).unwrap();
    assert_eq!(s.stored(), 2, "one combined operand per flush");
    assert_eq!(s.sum(), 1_200);
}

/// D287's point 0: a replay that spills (an arena too small for what it replays, #143/#904)
/// writes recovered memtables without GC, and the open flushes the rest the same way, so
/// no flush on the open path combines anything; the first flush after the open is a D287
/// flush whose other sources (the spilled SSTs) lie entirely below its memtable, and the
/// compaction after it combines them under its own `other_sources`. The counter reads its
/// sum throughout, and after another reopen.
#[test]
fn a_counter_replayed_through_a_spill_reads_its_sum() {
    use pigeonhole_io::sim::CrashKind;
    // 16 counters per commit, so the replay (about 80,000 entries) outgrows a small arena.
    const N: i64 = 5_000;
    const COLS: i64 = 16;
    let vfs = SimVfs::new(294);
    let open = |budget: u64| {
        let mut o: EngineOptions = common::options(Arc::clone(&vfs), 1, budget);
        o.pin_threads = false;
        o.wal.segment_size = 4 << 20;
        o.memtable_freeze_bytes = budget;
        o.wal_pin_bytes = u64::MAX;
        o.compaction.l0_trigger = u32::MAX;
        Engine::open(Path::new(DB), o).unwrap()
    };
    let db = open(16 << 20);
    let family = FamilyOptions::default()
        .merge_operator("pigeonhole.i64_add")
        .kind(FamilyKind::Counter);
    let info = db.create_table("t", &[("c".into(), family)]).unwrap();
    let (t, c) = (info.id, info.family("c").unwrap().id);
    let incr = |db: &Engine, d: i64| {
        let mut wb = WriteBatch::new();
        for q in 0..COLS {
            wb.merge(t, c, b"r", format!("n{q:02}").as_bytes(), ValueRef::I64(d))
                .unwrap();
        }
        db.commit(wb, Some(Durability::GroupSync)).unwrap();
    };
    let sum = |db: &Engine| -> i64 {
        let mut spec = ReadSpec::default();
        spec.families = vec![c];
        db.read_row(&db.snapshot().unwrap(), t, b"r", &spec)
            .unwrap()
            .map_or(0, |r| {
                r.cells
                    .iter()
                    .map(|cell| match cell.data.value() {
                        ValueRef::I64(v) => v,
                        v => panic!("not an i64: {v:?}"),
                    })
                    .sum()
            })
    };
    for _ in 0..N {
        incr(&db, 1);
    }
    assert_eq!(
        db.metrics().flushes,
        0,
        "everything is in the WAL, nothing flushed"
    );
    vfs.crash(CrashKind::Power);
    drop(db);

    // A 1 MiB arena cannot hold the replay: it spills, and the open flushes the rest.
    let db = open(1 << 20);
    assert_eq!(sum(&db), N * COLS, "after the spilled replay");
    assert!(
        db.sst_levels().len() >= 2,
        "the replay spilled: {:?}",
        db.sst_levels()
    );
    for _ in 0..1_000 {
        incr(&db, 2);
    }
    db.flush().unwrap();
    assert_eq!(
        sum(&db),
        N * COLS + 2_000 * COLS,
        "after a D287 flush above the spilled SSTs"
    );
    db.compact(None).unwrap();
    assert_eq!(sum(&db), N * COLS + 2_000 * COLS, "after compaction");
    db.close().unwrap();
    drop(db);
    let db = open(1 << 20);
    assert_eq!(sum(&db), N * COLS + 2_000 * COLS, "after a reopen");
    db.close().unwrap();
}
